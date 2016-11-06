// Copyright 2015 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package newton

import (
	"bytes"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/purak/httptimeout"
	"github.com/purak/newton/log"
)

var ErrAuthentication = errors.New("Authentication failed")

type AuthHandler struct {
	Handler      http.Handler
	callbackURL  string
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (h *AuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		// Browser does not send authentication headers in OPTIONS request.
		h.Handler.ServeHTTP(w, r)
		return
	}
	if len(h.callbackURL) != 0 {
		ah := r.Header.Get("X-Newton-Auth")
		if len(ah) == 0 {
			log.Debug("Missing header: X-Newton-Auth")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		code, err := h.authenticateClient(ah)
		if err != nil {
			log.Debugf("Authentication failed: %s. Status Code: %d", err, code)
			w.WriteHeader(code)
			return
		}
		log.Debug("Authentication successfull. Status Code: ", code)
	}
	h.Handler.ServeHTTP(w, r)
}

func (h *AuthHandler) authenticateClient(credentials string) (int, error) {
	var query = []byte(credentials)
	req, err := http.NewRequest("POST", h.callbackURL, bytes.NewBuffer(query))
	p, err := url.Parse(h.callbackURL)
	if err != nil {
		return 0, err
	}
	client := &http.Client{
		Transport: httptimeout.NewTransport(p.Host, h.readTimeout, h.writeTimeout),
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := client.Do(req)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, ErrAuthentication
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Errorf("Error while closing response body: %s", err)
		}
	}()
	return resp.StatusCode, nil
}
