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
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"io"
	"log"
	"os"
	"time"
)

// Run starts a routine to delete all rows from the specified database.
// If targetTables is not empty, it deletes from the specified tables.
// Otherwise, it deletes from all tables in the database.
// If excludeTables is not empty, those tables are excluded from the deleted tables.
// This function internally creates and uses a Cloud Spanner client.
func Run(ctx context.Context, projectID, instanceID, databaseID string, out io.Writer, whereClause string, targetTables, excludeTables []string) error {
	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)

	client, err := spanner.NewClient(ctx, database)
	if err != nil {
		return fmt.Errorf("failed to create Cloud Spanner client: %v", err)
	}
	defer func() {
		log.Printf("Closing spanner client...\n")
		client.Close()
	}()

	return RunWithClient(ctx, client, out, whereClause, targetTables, excludeTables)
}

// RunWithClient starts a routine to delete all rows using the given spanner client.
// If targetTables is not empty, it deletes from the specified tables.
// Otherwise, it deletes from all tables in the database.
// If excludeTables is not empty, those tables are excluded from the deleted tables.
// This function uses an externally passed Cloud Spanner client.
func RunWithClient(ctx context.Context, client *spanner.Client, out io.Writer, whereClause string, targetTables, excludeTables []string) error {
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

	log.Println("Fetching row counts from spanner...")

	for _, table := range flattenTables(coordinator.tables) {
		table.deleter.updateRowCount(ctx)
	}

	p := mpb.New()
	rowsToDelete := 0
	tables := flattenTables(coordinator.tables)
	for _, table := range tables {
		if table.deleter.totalRows > 0 {
			rowsToDelete += int(table.deleter.totalRows)
			print(fmt.Sprintf("%s rows from %s\n", formatNumber(table.deleter.totalRows), table.tableName))
		}
	}

	if rowsToDelete > 0 {
		if confirm(fmt.Sprintf("Rows in these tables matching `%s` will be deleted. Do you want to continue?", whereClause)) {

			coordinator.start(ctx)

			for _, table := range tables {
				if table.deleter.totalRows > 0 {
					bar := p.AddBar(int64(table.deleter.totalRows),
						mpb.PrependDecorators(
							decor.Name(table.tableName, decor.WC{C: decor.DindentRight | decor.DextraSpace}),
							decor.CountersNoUnit("(%d / %d)", decor.WCSyncWidth),
						),
						mpb.AppendDecorators(
							decor.OnComplete(
								decor.Percentage(decor.WC{W: 5}), "done",
							),
						),
					)
					go func() {
						for {
							switch table.deleter.status {
							case statusCompleted:
								bar.SetCurrent(int64(table.deleter.totalRows))
							case statusAnalyzing:
								// nop
							default:
								deletedRows := table.deleter.totalRows - table.deleter.remainedRows
								bar.SetCurrent(int64(deletedRows))
							}

							time.Sleep(time.Second * 1)
						}
					}()
				}
			}

			if err := coordinator.waitCompleted(); err != nil {
				return fmt.Errorf("failed to delete: %v", err)
			}

			p.Wait()

			log.Printf("Done! All rows matching `%s` have been deleted successfully.\n", whereClause)
		}
	} else {
		log.Printf("No rows found in these tables matching `%s`.\n", whereClause)
	}
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
