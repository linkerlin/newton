package kv

import (
	"bytes"

	"io"
	"io/ioutil"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/log"
)

func (k *KV) setHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")
	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if err = k.Set(key, value); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (k *KV) getHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")

	value, err := k.Get(key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	nr, err := io.Copy(w, bytes.NewReader(value))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Debugf("%d bytes has been written to network socket for key: %s", nr, key)
}

func (k *KV) deleteHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")

	err := k.Delete(key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Debugf("%s has been deleted", key)
}
