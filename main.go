package main

import (
	"database/sql"
	json "encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/ssrdive/scribe"
	"math"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ssrdive/mysequel"
	smodels "github.com/ssrdive/scribe/models"
)

func main() {
	fmt.Println("Running main")
	dsn := flag.String("dsn", "user:password@tcp(host)/database_name?parseTime=true",
		"MySQL data source name")
	option := flag.String("option", "None", "ri [Reverse Invoice]")
	payload := flag.String("payload", "{}",
		"ri = {'invoice_id':10, 'include_item_list': [1032, 3013]}")
	flag.Parse()

	fmt.Println(*dsn)
	db, err := openDB(*dsn)
	if err != nil {
		fmt.Println(fmt.Errorf("%v", err))
		os.Exit(-1)
	}

	switch *option {
	case "ri":
		err = reverseInvoice(db, *payload)
		if err != nil {
			fmt.Println(fmt.Errorf("%v", err))
			os.Exit(-1)
		}
		break
	case "None":
		fmt.Println(fmt.Errorf("no option given. exiting"))
		os.Exit(-1)
		break
	default:
		fmt.Println(fmt.Errorf("invalid option. exiting"))
		os.Exit(-1)
		break
	}
}

func reverseInvoice(db *sql.DB, payload string) error {

	type IncludeItemListLine struct {
		ItemID int `json:"item_id"`
		Qty    int `json:"qty"`
	}

	type ReverseInvoice struct {
		InvoiceID       int                   `json:"invoice_id"`
		IncludeItemList []IncludeItemListLine `json:"include_item_list"`
	}

	type InvoiceItem struct {
		WarehouseID         int
		ItemID              int
		GoodsReceivedNoteID int
		InventoryTransferID sql.NullInt32
		Qty                 int
		CostPrice           float64
		Price               float64
	}

	type CurrentStockLine struct {
		WarehouseID         int
		ItemID              int
		GoodsReceivedNoteID int
		InventoryTransferID sql.NullInt32
		Qty                 int
		Price               float64
	}

	var args ReverseInvoice
	err := json.Unmarshal([]byte(payload), &args)
	if err != nil {
		return err
	}

	fmt.Printf("Agrs: %+v\n", args)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		_ = tx.Commit()
	}()

	var invoiceItems []InvoiceItem

	if len(args.IncludeItemList) > 0 {
		var includeItemList = make([]interface{}, len(args.IncludeItemList))

		for i, item := range args.IncludeItemList {
			includeItemList[i] = item.ItemID
		}

		err = mysequel.QueryToStructs(&invoiceItems, tx, fmt.Sprintf(`SELECT I.warehouse_id, item_id, 
       		goods_received_note_id, inventory_transfer_id, qty, invoice_item.cost_price, invoice_item.price
			FROM invoice_item
			LEFT JOIN invoice I ON I.id = invoice_item.invoice_id
			WHERE invoice_id = ? AND item_id IN (%v)`, ConvertArrayToString(includeItemList)), args.InvoiceID)
	} else {
		err = mysequel.QueryToStructs(&invoiceItems, tx, `SELECT I.warehouse_id, item_id, goods_received_note_id, 
       		inventory_transfer_id, qty, invoice_item.cost_price, invoice_item.price
			FROM invoice_item
			LEFT JOIN invoice I ON I.id = invoice_item.invoice_id
			WHERE invoice_id = ?`, args.InvoiceID)
	}

	if err != nil {
		return err
	}

	if len(invoiceItems) <= 0 {
		return errors.New("could not retrieve items from invoice to be reversed")
	}

	// Select items reversal

	costPrice := 0.0
	sellingPrice := 0.0
	if len(args.IncludeItemList) > 0 {

		var selectInvoiceItems []InvoiceItem

		for _, includeItem := range args.IncludeItemList {
			qty := includeItem.Qty

			for _, invoiceItem := range invoiceItems {
				if qty == 0 {
					break
				}

				if invoiceItem.Qty == 0 {
					continue
				}

				if invoiceItem.ItemID == includeItem.ItemID {
					if invoiceItem.Qty > qty {
						appendInvoiceItem := invoiceItem
						appendInvoiceItem.Qty = qty
						selectInvoiceItems = append(selectInvoiceItems, appendInvoiceItem)
						costPrice += invoiceItem.CostPrice * float64(qty)
						sellingPrice += invoiceItem.Price * float64(qty)
						qty = 0
					} else {
						selectInvoiceItems = append(selectInvoiceItems, invoiceItem)
						costPrice += invoiceItem.CostPrice * float64(invoiceItem.Qty)
						sellingPrice += invoiceItem.Price * float64(invoiceItem.Qty)
						qty = qty - invoiceItem.Qty
					}
				}
			}

			if qty != 0 {
				fmt.Printf("Failed item: %+v\n", includeItem)
				return errors.New("unable to return the specified quantity")
			}
		}

		invoiceItems = selectInvoiceItems
	}

	fmt.Printf("Invoice Items: %+v\n\n", invoiceItems)

	// There can be multiple invoice lines for the same item
	for _, item := range invoiceItems {
		fmt.Printf("\nItem: \t\t\t%+v\n", item)

		var stockLine CurrentStockLine
		if item.InventoryTransferID.Valid {
			err = tx.QueryRow(`SELECT warehouse_id, item_id, goods_received_note_id, inventory_transfer_id, qty, 
       			price 
				FROM current_stock WHERE warehouse_id = ? AND item_id = ? AND goods_received_note_id = ? 
				AND inventory_transfer_id = ?`, item.WarehouseID, item.ItemID, item.GoodsReceivedNoteID,
				item.InventoryTransferID.Int32).
				Scan(&stockLine.WarehouseID, &stockLine.ItemID,
					&stockLine.GoodsReceivedNoteID, &stockLine.InventoryTransferID, &stockLine.Qty, &stockLine.Price)
			if err != nil {
				return err
			}
			fmt.Printf("Stock Line Before: \t%+v\n", stockLine)

			_, err = tx.Exec(`UPDATE current_stock SET qty = qty + ? 
                WHERE warehouse_id = ? AND item_id = ? AND goods_received_note_id = ? 
				AND inventory_transfer_id = ?`, item.Qty, item.WarehouseID, item.ItemID, item.GoodsReceivedNoteID,
				item.InventoryTransferID.Int32)
			if err != nil {
				return err
			}

			err = tx.QueryRow(`SELECT warehouse_id, item_id, goods_received_note_id, inventory_transfer_id, qty, 
       			price 
				FROM current_stock WHERE warehouse_id = ? AND item_id = ? AND goods_received_note_id = ? 
				AND inventory_transfer_id = ?`, item.WarehouseID, item.ItemID, item.GoodsReceivedNoteID,
				item.InventoryTransferID.Int32).
				Scan(&stockLine.WarehouseID, &stockLine.ItemID,
					&stockLine.GoodsReceivedNoteID, &stockLine.InventoryTransferID, &stockLine.Qty, &stockLine.Price)
			if err != nil {
				return err
			}
			fmt.Printf("Stock Line After: \t%+v\n\n", stockLine)

			if len(args.IncludeItemList) > 0 {
				_, err = tx.Exec(`UPDATE invoice_item SET qty = qty - ? WHERE invoice_id = ? AND item_id = ?
					AND goods_received_note_id = ? AND inventory_transfer_id = ?`, item.Qty, args.InvoiceID,
					item.ItemID, item.GoodsReceivedNoteID, item.InventoryTransferID.Int32)
				if err != nil {
					return err
				}
			}
		} else {
			err = tx.QueryRow(`SELECT warehouse_id, item_id, goods_received_note_id, inventory_transfer_id, qty, 
       			price 
				FROM current_stock WHERE warehouse_id = ? AND item_id = ? AND goods_received_note_id = ? 
				AND inventory_transfer_id IS NULL`, item.WarehouseID, item.ItemID, item.GoodsReceivedNoteID).
				Scan(&stockLine.WarehouseID, &stockLine.ItemID,
					&stockLine.GoodsReceivedNoteID, &stockLine.InventoryTransferID, &stockLine.Qty, &stockLine.Price)
			if err != nil {
				return err
			}
			fmt.Printf("Stock Line Before: \t%+v\n", stockLine)

			_, err = tx.Exec(`UPDATE current_stock SET qty = qty + ? 
                WHERE warehouse_id = ? AND item_id = ? AND goods_received_note_id = ? 
				AND inventory_transfer_id IS NULL`, item.Qty, item.WarehouseID, item.ItemID, item.GoodsReceivedNoteID)
			if err != nil {
				return err
			}

			err = tx.QueryRow(`SELECT warehouse_id, item_id, goods_received_note_id, inventory_transfer_id, qty, 
       			price 
				FROM current_stock WHERE warehouse_id = ? AND item_id = ? AND goods_received_note_id = ? 
				AND inventory_transfer_id IS NULL`, item.WarehouseID, item.ItemID, item.GoodsReceivedNoteID).
				Scan(&stockLine.WarehouseID, &stockLine.ItemID,
					&stockLine.GoodsReceivedNoteID, &stockLine.InventoryTransferID, &stockLine.Qty, &stockLine.Price)
			if err != nil {
				return err
			}
			fmt.Printf("Stock Line After: \t%+v\n\n", stockLine)

			if len(args.IncludeItemList) > 0 {
				_, err = tx.Exec(`UPDATE invoice_item SET qty = qty - ? WHERE invoice_id = ? AND item_id = ?
					AND goods_received_note_id = ? AND inventory_transfer_id IS NULL`, item.Qty, args.InvoiceID,
					item.ItemID, item.GoodsReceivedNoteID)
				if err != nil {
					return err
				}
			}
		}
	}

	if len(args.IncludeItemList) > 0 {
		fmt.Printf("Cost price: %.2f\n", costPrice)
		fmt.Printf("Selling price: %.2f\n", sellingPrice)

		var discount float64
		var cashInHandAccount int
		err = tx.QueryRow(`SELECT I.discount, U.account_id
			FROM invoice I
			LEFT JOIN user U ON U.id = I.user_id
			WHERE I.id = ?`, args.InvoiceID).Scan(&discount, &cashInHandAccount)
		if err != nil {
			return err
		}

		finalSoldPrice := math.Round((sellingPrice*(float64(100)-discount)/100)*100) / 100
		fmt.Printf("Final sold price: %.2f\n", finalSoldPrice)

		_, err = tx.Exec(`UPDATE invoice SET cost_price = cost_price - ?, 
		   price_before_discount = price_before_discount - ?, price_after_discount = price_after_discount - ?
		   WHERE id = ?`, costPrice, sellingPrice, finalSoldPrice, args.InvoiceID)
		if err != nil {
			return err
		}

		const (
			SparePartsSalesAccountID       = 200
			SparePartsCostOfSalesAccountID = 202
			StockAccountID                 = 183
		)

		journalEntries := []smodels.JournalEntry{
			{Account: fmt.Sprintf("%d", SparePartsSalesAccountID), Debit: fmt.Sprintf("%f",
				finalSoldPrice), Credit: ""},
			{Account: fmt.Sprintf("%d", cashInHandAccount), Debit: "",
				Credit: fmt.Sprintf("%f", finalSoldPrice)},
			{Account: fmt.Sprintf("%d", StockAccountID), Debit: fmt.Sprintf("%f", costPrice),
				Credit: ""},
			{Account: fmt.Sprintf("%d", SparePartsCostOfSalesAccountID), Debit: "",
				Credit: fmt.Sprintf("%f", costPrice)},
		}

		tid, err := mysequel.Insert(mysequel.Table{
			TableName: "transaction",
			Columns:   []string{"user_id", "datetime", "posting_date", "remark"},
			Vals: []interface{}{1, time.Now().Format("2006-01-02 15:04:05"),
				time.Now().Format("2006-01-02"),
				fmt.Sprintf("INVOICE REVERSAL %d\nPayload - %+v", args.InvoiceID, args)},
			Tx: tx,
		})
		if err != nil {
			return err
		}

		err = scribe.IssueJournalEntries(tx, tid, journalEntries)
		if err != nil {
			return err
		}

		fmt.Printf("Select items reversal complete\n")
	} else {
		_, err = tx.Exec("DELETE FROM invoice_item WHERE invoice_id = ?", args.InvoiceID)
		if err != nil {
			return err
		}

		_, err = tx.Exec("DELETE FROM invoice WHERE id = ?", args.InvoiceID)
		if err != nil {
			return err
		}

		txnRemark := fmt.Sprintf("INVOICE %d", args.InvoiceID)
		var txnID int
		err = tx.QueryRow("SELECT id FROM transaction WHERE remark = ?", txnRemark).Scan(&txnID)
		fmt.Printf("Deleting transaction: %d\n", txnID)

		_, err = tx.Exec("DELETE FROM account_transaction WHERE transaction_id = ?", txnID)
		if err != nil {
			return err
		}

		_, err = tx.Exec("DELETE FROM transaction WHERE id = ?", txnID)
		if err != nil {
			return err
		}
	}

	return nil
}

func ConvertArrayToString(arr []interface{}) string {
	str := ""
	for i, elem := range arr {
		if i != len(arr)-1 {
			str = str + fmt.Sprintf("%v", elem) + ","
		} else {
			str = str + fmt.Sprintf("%v", elem)
		}
	}
	return str
}

func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, err
}
