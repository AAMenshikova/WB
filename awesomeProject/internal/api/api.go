package api

import (
	"awesomeProject/internal/model"
	"awesomeProject/internal/service"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

func HandlerGet(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/order/")
		if id == "" || id == r.URL.Path {
			http.Error(w, "use /order/{order_uid}", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		order, err := svc.GetOrderByID(ctx, id)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(order)
	}
}

func HandlerPost(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		var order model.Order
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if order.Order_uid == "" {
			http.Error(w, "order_uid is required", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := svc.UpsertOrder(ctx, order); err != nil {
			http.Error(w, "failed to insert order", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "order_uid": order.Order_uid})
	}
}
