package partition

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/log"
)

type AlivenessMsg struct {
	Birthdate int64 `json:"birthdate"`
}

func (p *Partition) alivenessHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if r.Method == http.MethodHead {
		// Internal aliveness check
		return
	}
	msg := &AlivenessMsg{
		Birthdate: p.birthdate,
	}
	if err := json.NewEncoder(w).Encode(msg); err != nil {
		p.jsonErrorResponse(w, fmt.Sprintf("Error while returning birthdate: %s", err), http.StatusInternalServerError)
	}
}

// ErrorMsg represents an error message
type ErrorMsg struct {
	Description string `json:"description"`
}

func (p *Partition) partitionSetHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	dec := gob.NewDecoder(r.Body) // Will read from network.
	var table partitionTable
	err := dec.Decode(&table)
	if err != nil {
		p.jsonErrorResponse(w, fmt.Sprintf("Error while setting partition table: %s", err), http.StatusInternalServerError)
		return
	}
	partitionTableLock.Lock()
	defer partitionTableLock.Unlock()

	// Check the sender
	cn := p.getCoordinatorMember()
	cAddr := table.Sorted[0].Addr
	if cn != cAddr {
		log.Warnf("%s tried to set a partition table. Rejected!", cAddr)
		w.WriteHeader(http.StatusConflict)
		return
	}
	log.Infof("Partition table received from coordinator node: %s", cAddr)
	p.table = &table

	// Remove stale members
	list := make(map[string]struct{})
	for _, item := range p.table.Sorted {
		list[item.Addr] = struct{}{}
	}
	for _, addr := range p.getMemberList2() {
		if _, ok := list[addr]; !ok {
			err = p.deleteMember(addr)
			if err == errMemberNotFound {
				err = nil
			}
			if err != nil {
				log.Errorf("Error while deleting stale member from discovery subsystem: %s", err)
			}
		}
	}

	for _, item := range table.Sorted {
		if item.Addr != p.config.Address {
			// We may catch a restarted member. Check the birhdate.
			if !p.checkMemberWithBirthdate(item.Addr, item.Birthdate) {
				err = p.deleteMember(item.Addr)
				if err == errMemberNotFound {
					err = nil
				}
				if err != nil {
					log.Errorf("Error while deleting member from discovery subsystem: %s", err)
				}
			}
			err = p.addMember(item.Addr, "", item.Birthdate)
			if err == errMemberAlreadyExist {
				err = nil
			}
			if err != nil {
				log.Errorf("Error while adding member to discovery subsystem: %s", err)
			}
		}
	}

	select {
	case <-p.nodeInitialized:
		return
	default:
	}
	close(p.nodeInitialized)
}

// jsonErrorResponse prepares a json response with the given description and  status code variables then returns the response.
func (p *Partition) jsonErrorResponse(w http.ResponseWriter, desc string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	msg := &ErrorMsg{
		Description: desc,
	}
	if err := json.NewEncoder(w).Encode(msg); err != nil {
		log.Errorf("Error while returning error message: %s", err)
	}
}

type suspiciousMembers struct {
	mu sync.RWMutex

	m map[string]struct{}
}

func (p *Partition) checkMemberHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if r.Body == nil {
		p.jsonErrorResponse(w, "Send a request body", http.StatusBadRequest)
		return
	}
	var c CheckMember
	err := json.NewDecoder(r.Body).Decode(&c)
	if err != nil {
		p.jsonErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}
	p.tryCheckSuspiciousMember(c.Member)
}

func (p *Partition) tryCheckSuspiciousMember(addr string) {
	p.suspiciousMembers.mu.Lock()
	_, ok := p.suspiciousMembers.m[addr]
	if !ok {
		// TODO: We keep members in a map with their birthdate for god's sake.
		partitionTableLock.RLock()
		for _, item := range p.table.Sorted {
			if item.Addr != addr {
				continue
			}
			p.waitGroup.Add(1)
			p.suspiciousMembers.m[addr] = struct{}{}
			go p.checkSuspiciousMember(addr, item.Birthdate)
		}
		partitionTableLock.RUnlock()
	}
	p.suspiciousMembers.mu.Unlock()
}
