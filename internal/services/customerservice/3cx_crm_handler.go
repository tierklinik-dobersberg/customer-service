package customerservice

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/nyaruka/phonenumbers"
)

type CRMLookupResponse struct {
	FirstName    string   `json:"firstName"`
	LastName     string   `json:"lastName"`
	ID           string   `json:"id"`
	PhoneNumbers []string `json:"phoneNumbers"`
}

// GET /crm/lookup?phone=xyz
func (svc *CustomerService) CRMLookupHandler(w http.ResponseWriter, req *http.Request) {
	phone := req.URL.Query().Get("phone")

	if phone == "" {
		http.Error(w, "missing phone number", http.StatusBadRequest)
		return
	}

	formatted := phone

	parsed, err := phonenumbers.Parse(phone, "AT")
	if err != nil {
		slog.ErrorContext(req.Context(), "3cx provided an invalid phone number", slog.Any("error", err.Error()))
	} else {
		formatted = phonenumbers.Format(parsed, phonenumbers.INTERNATIONAL)
	}

	res, err := svc.repo.LookupCustomerByPhone(req.Context(), formatted)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(res) == 0 {
		http.Error(w, "customer not found", http.StatusNotFound)
		return
	}

	response := CRMLookupResponse{
		ID:           res[0].Customer.Id,
		FirstName:    res[0].Customer.FirstName,
		LastName:     res[0].Customer.LastName,
		PhoneNumbers: make([]string, len(res[0].Customer.PhoneNumbers)),
	}

	for idx, p := range res[0].Customer.PhoneNumbers {
		response.PhoneNumbers[idx] = strings.ReplaceAll(p, " ", "")
	}

	blob, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(blob); err != nil {
		slog.ErrorContext(req.Context(), "failed to write crm lookup response", slog.Any("error", err.Error()))
	}
}
