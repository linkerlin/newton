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
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/purak/newton/store"
)

func setupCS() (ClientStore, error) {
	p := "/tmp/testNewtonDB.newton"
	err := os.RemoveAll(p)
	if err != nil {
		return nil, err
	}
	pth := path.Join(p, newtonDB)
	str, err := store.OpenLevelDB(pth)
	if err != nil {
		return nil, fmt.Errorf("%s: %s", pth, err)
	}
	return NewClientStore(str), nil
}

func tearDownCS() error {
	p := "/tmp/testNewtonDB.newton"
	return os.RemoveAll(p)
}

func TestAddClient(t *testing.T) {
	userHash := "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12"
	conn := &connection{}
	n, err := setupCS()
	if err != nil {
		t.Error("Expected nil. Got: ", err)
	}

	fn := func() {
		if _, err := n.AddClient(userHash, conn); err != nil {
			t.Errorf("Expected nil. Got %s", err)
		}
	}
	go fn()
	go fn()
	// Wait to acquire locks
	time.Sleep(1 * time.Millisecond)
	clients := n.GetClients(userHash)
	if len(clients) != 2 {
		t.Error("Expected: 2 clients. Actual: %s", len(clients))
	}

	err = tearDownCS()
	if err != nil {
		t.Error("Expected nil. Got: ", err)
	}
}

func TestDelClient(t *testing.T) {
	userHash := "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12"
	conn := &connection{}
	n, err := setupCS()
	if err != nil {
		t.Error("Expected nil. Got: ", err)
	}

	clientID, err := n.AddClient(userHash, conn)
	if err != nil {
		t.Error("Expected nil. Got: ", err)
	}
	n.DelClient(userHash, clientID)
	clients := n.GetClients(userHash)
	if len(clients) != 0 {
		t.Error("Expected: 0 clients. Actual: %s", len(clients))
	}

	err = tearDownCS()
	if err != nil {
		t.Error("Expected nil. Got: ", err)
	}
}

func TestGetClient(t *testing.T) {
	userHash := "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12"
	conn := &connection{}
	n, err := setupCS()
	if err != nil {
		t.Error("Expected nil. Got: ", err)
	}

	clientID, err := n.AddClient(userHash, conn)
	if err != nil {
		t.Error("Expected nil. Got: ", err)
	}
	cl := n.GetClient(userHash, clientID)

	if cl != conn {
		t.Error("Returned a different connection object.")
	}
	err = tearDownCS()
	if err != nil {
		t.Error("Expected nil. Got: ", err)
	}
}
