//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// Package truncate provides the functionality to truncate all rows from a Cloud Spanner database.
package truncate

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/davidsnyder/uiprogress"
)

// Run starts a routine to delete all rows from the specified database.
// If targetTables is not empty, it deletes from the specified tables.
// Otherwise, it deletes from all tables in the database.
// If excludeTables is not empty, those tables are excluded from the deleted tables.
// This function internally creates and uses a Cloud Spanner client.
func Run(ctx context.Context, projectID, instanceID, databaseID string, quiet bool, out io.Writer, whereClause string, targetTables, excludeTables []string) error {
	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)

	client, err := spanner.NewClient(ctx, database)
	if err != nil {
		return fmt.Errorf("failed to create Cloud Spanner client: %v", err)
	}
	defer func() {
		log.Printf("Closing spanner client...\n")
		client.Close()
	}()

	return RunWithClient(ctx, client, quiet, out, whereClause, targetTables, excludeTables)
}

// RunWithClient starts a routine to delete all rows using the given spanner client.
// If targetTables is not empty, it deletes from the specified tables.
// Otherwise, it deletes from all tables in the database.
// If excludeTables is not empty, those tables are excluded from the deleted tables.
// This function uses an externally passed Cloud Spanner client.
func RunWithClient(ctx context.Context, client *spanner.Client, quiet bool, out io.Writer, whereClause string, targetTables, excludeTables []string) error {
	log.SetOutput(out)
	log.Printf("Fetching table schema from %s\n", client.DatabaseName())
	schemas, err := fetchTableSchemas(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to fetch table schema: %v", err)
	}

	schemas, err = filterTableSchemas(schemas, targetTables, excludeTables)
	if err != nil {
		return fmt.Errorf("failed to filter table schema: %v", err)
	}

	indexes, err := fetchIndexSchemas(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to fetch index schema: %v", err)
	}

	coordinator, err := newCoordinator(schemas, indexes, client, whereClause)
	if err != nil {
		return fmt.Errorf("failed to coordinate: %v", err)
	}

	for _, table := range flattenTables(coordinator.tables) {
		table.deleter.updateRowCount(ctx)
	}

	// Show progress bars.
	progress := uiprogress.New()
	progress.SetOut(out)
	progress.SetRefreshInterval(time.Millisecond * 500)
	var maxNameLength int
	for _, schema := range schemas {
		if l := len(schema.tableName); l > maxNameLength {
			maxNameLength = l
		}
	}
	for _, table := range flattenTables(coordinator.tables) {
		showProgressBar(progress, table, maxNameLength)
	}

	time.Sleep(time.Second)
	progress.Print()

	if !quiet {
		if !confirm(fmt.Sprintf("Rows in these tables matching `%s` will be deleted. Do you want to continue?", whereClause)) {
			return nil
		}
	} else {
		log.Printf("Rows in these tables will be deleted.\n")
	}

	progress.Start()
	coordinator.start(ctx)

	if err := coordinator.waitCompleted(); err != nil {
		progress.Stop()
		return fmt.Errorf("failed to delete: %v", err)
	}
	// Wait for reflecting the latest progresses to progress bars.
	time.Sleep(time.Second)
	progress.Stop()

	log.Printf("Done! All rows matching `%s` have been deleted successfully.\n", whereClause)
	return nil
}

// confirm returns true if a user confirmed the message, otherwise returns false.
func confirm(msg string) bool {
	log.Printf("%s [Y/n] ", msg)
	s := bufio.NewScanner(os.Stdin)
	for {
		s.Scan()
		switch s.Text() {
		case "Y":
			return true
		case "n":
			return false
		default:
			log.Printf("Please answer Y or n: ")
		}
	}
}

func showProgressBar(progress *uiprogress.Progress, table *table, maxNameLength int) {
	bar := progress.AddBar(100)
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		elapsed := int(b.TimeElapsed().Seconds())
		return fmt.Sprintf("%5ds", elapsed)
	})
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		var s string
		switch table.deleter.status {
		case statusAnalyzing:
			s = "analyzing"
		case statusWaiting:
			s = "waiting  " // append space for alignment
		case statusDeleting, statusCascadeDeleting:
			s = "deleting " // append space for alignment
		case statusCompleted:
			s = "completed"
		}
		return fmt.Sprintf("%-*s%s", maxNameLength+2, table.tableName+": ", s)
	})
	bar.AppendCompleted()
	bar.AppendFunc(func(b *uiprogress.Bar) string {
		deletedRows := table.deleter.totalRows - table.deleter.remainedRows
		return fmt.Sprintf("(%s / %s)", formatNumber(deletedRows), formatNumber(table.deleter.totalRows))
	})

	// HACK: We call progressBar.Incr() to start timer in the progress bar.
	bar.Set(-1)
	bar.Incr()

	// Update progress periodically.
	go func() {
		for {
			switch table.deleter.status {
			case statusCompleted:
				// Increment the progress bar until it reaches 100
				for bar.Incr() {
				}
			case statusAnalyzing:
				// nop
			default:
				deletedRows := table.deleter.totalRows - table.deleter.remainedRows
				target := int(float32(deletedRows) / float32(table.deleter.totalRows) * 100)
				for i := bar.Current(); i < target; i++ {
					bar.Incr()
				}
			}

			time.Sleep(time.Second * 1)
		}
	}()
}
