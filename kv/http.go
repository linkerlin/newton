package kv

import (
	"bytes"
	"strconv"
	"strings"

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
		log.Errorf("[HTTP-KV-Set] Error while reading value for key %s: %s", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var ttl int64
	ttlRaw := ps.ByName("ttl")
	if len(strings.Trim(ttlRaw, " ")) != 0 {
		ttl, err = strconv.ParseInt(ttlRaw, 10, 64)
		if err != nil {
			log.Errorf("[HTTP-KV-Set] Error while reading ttl for key %s: %s", key, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	if err = k.Set(key, value, ttl); err != nil {
		log.Errorf("[HTTP-KV-Set] Error while setting key %s: %s", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Debugf("%s has been set", key)
}

func (k *KV) getHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")

	value, err := k.Get(key)
	if err != nil {
		log.Errorf("[HTTP-KV-Get] Error while reading value for key %s: %s", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	nr, err := io.Copy(w, bytes.NewReader(value))
	if err != nil {
		log.Errorf("[HTTP-KV-Get] Error while sending value for key %s: %s", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Debugf("%d bytes has been written to network socket for key: %s", nr, key)
}

func (k *KV) deleteHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := ps.ByName("key")

	err := k.Delete(key)
	if err != nil {
		log.Errorf("[HTTP-KV-Delete] Error while deleting key %s: %s", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Debugf("%s has been deleted", key)
}
