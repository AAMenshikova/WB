package repository

import (
	"awesomeProject/internal/model"
	"context"
	"database/sql"
	"errors"
	"time"
)

var ErrNotFound = errors.New("order not found")

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (repo *Repository) InsertOrder(ctx context.Context, order model.Order) error {
	tx, err := repo.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, "INSERT INTO orders (order_uid, track_number, entry, locale, "+
		"internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)"+
		"VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (order_uid) DO NOTHING;",
		order.Order_uid, order.Track_number, order.Entry, order.Locale, order.Internal_signature,
		order.Customer_id, order.Delivery_service, order.Shardkey, order.Sm_id, order.Date_created, order.Oof_shard)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO delivery (order_uid, \"name\", phone, zip, city, address, region, email) "+
		"VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (order_uid) DO NOTHING;",
		order.Order_uid, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip, order.Delivery.City,
		order.Delivery.Address, order.Delivery.Region, order.Delivery.Email)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO payment (order_uid, \"transaction\", request_id, currency, provider, amount,"+
		" payment_dt, bank, delivery_cost, goods_total, custom_fee) "+
		"VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (order_uid) DO NOTHING;",
		order.Order_uid, order.Payment.Transaction, order.Payment.Request_id, order.Payment.Currency,
		order.Payment.Provider, order.Payment.Amount, order.Payment.Payment_dt,
		order.Payment.Bank, order.Payment.Delivery_cost, order.Payment.Goods_total, order.Payment.Custom_fee)
	if err != nil {
		return err
	}

	for _, item := range order.Items {
		_, err = tx.ExecContext(ctx, "INSERT INTO items (order_uid, chrt_id, track_number, price, rid, \"name\", sale, "+
			"\"size\", total_price, nm_id, brand, status) "+
			"VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (order_uid, chrt_id) DO NOTHING",
			order.Order_uid, item.Chrt_id, item.Track_number, item.Price,
			item.Rid, item.Name, item.Sale, item.Size, item.Total_price,
			item.Nm_id, item.Brand, item.Status)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (repo *Repository) GetOrderById(ctx context.Context, id string) (model.Order, error) {
	var order model.Order
	row := repo.db.QueryRowContext(ctx, "SELECT order_uid, track_number, entry, locale, internal_signature,"+
		"customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard FROM orders WHERE order_uid=$1", id)
	err := row.Scan(&order.Order_uid, &order.Track_number, &order.Entry, &order.Locale, &order.Internal_signature,
		&order.Customer_id, &order.Delivery_service, &order.Shardkey, &order.Sm_id, &order.Date_created, &order.Oof_shard)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Order{}, ErrNotFound
		}
		return order, err
	}

	row = repo.db.QueryRowContext(ctx, "SELECT \"name\", phone, zip, city, address, region, email"+
		" FROM delivery WHERE order_uid=$1", id)
	_ = row.Scan(&order.Delivery.Name, &order.Delivery.Phone, &order.Delivery.Zip,
		&order.Delivery.City, &order.Delivery.Address, &order.Delivery.Region, &order.Delivery.Email)

	row = repo.db.QueryRowContext(ctx, "SELECT \"transaction\", request_id, currency, provider, amount, "+
		"payment_dt, bank, delivery_cost, goods_total, custom_fee FROM payment WHERE order_uid=$1", id)
	_ = row.Scan(&order.Payment.Transaction, &order.Payment.Request_id, &order.Payment.Currency,
		&order.Payment.Provider, &order.Payment.Amount, &order.Payment.Payment_dt, &order.Payment.Bank,
		&order.Payment.Delivery_cost, &order.Payment.Goods_total, &order.Payment.Custom_fee)

	rows, err := repo.db.QueryContext(ctx, "SELECT chrt_id, track_number, price, rid, \"name\", sale, "+
		"\"size\", total_price, nm_id, brand, status FROM items WHERE order_uid=$1", id)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var item model.Items
			if err := rows.Scan(&item.Chrt_id, &item.Track_number, &item.Price, &item.Rid, &item.Name,
				&item.Sale, &item.Size, &item.Total_price, &item.Nm_id, &item.Brand, &item.Status); err == nil {
				order.Items = append(order.Items, item)
			}
		}
	}
	return order, nil
}

func (repo *Repository) LoadAll(ctx context.Context) ([]model.Order, error) {
	const q = `SELECT o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature, o.customer_id, 
	o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard, d.name, d.phone, d.zip, d.city, d.address,
	d.region, d.email, p.transaction, p.request_id, p.currency, p.provider, p.amount, p.payment_dt,
    p.bank, p.delivery_cost, p.goods_total, p.custom_fee, i.chrt_id, i.track_number, i.price, i.rid, i.name, i.sale,
	i.size, i.total_price, i.nm_id, i.brand, i.status 
    FROM orders as o
    JOIN delivery as d on o.order_uid = d.order_uid
    JOIN payment as p on o.order_uid = p.order_uid
    LEFT JOIN items as i on o.order_uid = i.order_uid
    ORDER BY o.order_uid;`

	rows, err := repo.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type rowType struct {
		orderUID, trackNumber, entry, locale, internalSignature, customerID string
		deliveryService, shardkey                                           string
		smID                                                                sql.NullInt64
		oofShard                                                            string
		dateCreated                                                         time.Time
		dName, dPhone, dZip, dCity, dAddress, dRegion, dEmail               string
		pTransaction, pRequestID, pCurrency, pProvider                      string
		pAmount, pPaymentDT                                                 sql.NullInt64
		pBank                                                               string
		pDeliveryCost, pGoodsTotal, pCustomFee                              sql.NullInt64
		iChrtID                                                             sql.NullInt64
		iTrackNumber, iRID, iName, iSize, iBrand                            sql.NullString
		iPrice, iSale, iTotalPrice, iNmID, iStatus                          sql.NullInt64
	}
	acc := make(map[string]*model.Order)
	for rows.Next() {
		var rrow rowType
		if err := rows.Scan(&rrow.orderUID, &rrow.trackNumber, &rrow.entry, &rrow.locale, &rrow.internalSignature,
			&rrow.customerID, &rrow.deliveryService, &rrow.shardkey, &rrow.smID, &rrow.dateCreated, &rrow.oofShard,
			&rrow.dName, &rrow.dPhone, &rrow.dZip, &rrow.dCity, &rrow.dAddress, &rrow.dRegion, &rrow.dEmail,
			&rrow.pTransaction, &rrow.pRequestID, &rrow.pCurrency, &rrow.pProvider, &rrow.pAmount, &rrow.pPaymentDT,
			&rrow.pBank, &rrow.pDeliveryCost, &rrow.pGoodsTotal, &rrow.pCustomFee,
			&rrow.iChrtID, &rrow.iTrackNumber, &rrow.iPrice, &rrow.iRID, &rrow.iName, &rrow.iSale, &rrow.iSize,
			&rrow.iTotalPrice, &rrow.iNmID, &rrow.iBrand, &rrow.iStatus,
		); err != nil {
			return nil, err
		}
		order, ok := acc[rrow.orderUID]
		if !ok {
			order = &model.Order{
				Order_uid:          rrow.orderUID,
				Track_number:       rrow.trackNumber,
				Entry:              rrow.entry,
				Locale:             rrow.locale,
				Internal_signature: rrow.internalSignature,
				Customer_id:        rrow.customerID,
				Delivery_service:   rrow.deliveryService,
				Shardkey:           rrow.shardkey,
				Sm_id:              int(rrow.smID.Int64),
				Date_created:       rrow.dateCreated,
				Oof_shard:          rrow.oofShard,
				Delivery: model.Delivery{
					Name:    rrow.dName,
					Phone:   rrow.dPhone,
					Zip:     rrow.dZip,
					City:    rrow.dCity,
					Address: rrow.dAddress,
					Region:  rrow.dRegion,
					Email:   rrow.dEmail,
				},
				Payment: model.Payment{
					Transaction:   rrow.pTransaction,
					Request_id:    rrow.pRequestID,
					Currency:      rrow.pCurrency,
					Provider:      rrow.pProvider,
					Amount:        int(rrow.pAmount.Int64),
					Payment_dt:    int(rrow.pPaymentDT.Int64),
					Bank:          rrow.pBank,
					Delivery_cost: int(rrow.pDeliveryCost.Int64),
					Goods_total:   int(rrow.pGoodsTotal.Int64),
					Custom_fee:    int(rrow.pCustomFee.Int64),
				},
				Items: make([]model.Items, 0, 4),
			}
			acc[rrow.orderUID] = order
		}
		if rrow.iChrtID.Valid {
			order.Items = append(order.Items, model.Items{
				Chrt_id:      int(rrow.iChrtID.Int64),
				Track_number: rrow.iTrackNumber.String,
				Price:        int(rrow.iPrice.Int64),
				Rid:          rrow.iRID.String,
				Name:         rrow.iName.String,
				Sale:         int(rrow.iSale.Int64),
				Size:         rrow.iSize.String,
				Total_price:  int(rrow.iTotalPrice.Int64),
				Nm_id:        int(rrow.iNmID.Int64),
				Brand:        rrow.iBrand.String,
				Status:       int(rrow.iStatus.Int64),
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]model.Order, 0, len(acc))
	for _, o := range acc {
		out = append(out, *o)
	}
	return out, nil
}
