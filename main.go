package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"os/user"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"

	"github.com/miekg/dns"
)

var nixopsStateDb *sql.DB

func openNixopsStateDb() *sql.DB {
	usr, _ := user.Current()
	db, err := sql.Open("sqlite3", filepath.Join(
		usr.HomeDir, ".nixops/deployments.nixops")+"?mode=ro")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func nixopsHostIp(deployment string, hostname string) (net.IP, error) {
	var ip string
	row := nixopsStateDb.QueryRow(`
            SELECT RA.value AS ipv4
	    FROM Resources R
	    INNER JOIN ResourceAttrs RA ON RA.machine = R.id AND RA.name = 'privateIpv4'
	    INNER JOIN DeploymentAttrs DA ON DA.deployment = R.deployment AND DA.name = 'name'
	    WHERE R.name = ?
	    AND DA.value = ?;
    `, hostname, deployment)

	if err := row.Scan(&ip); err != nil {
		return nil, fmt.Errorf("Error while trying to find host '%s' in deployment '%s' in NixOps: %q",
			hostname, deployment, err)
	}
	return net.ParseIP(ip), nil
}

func domainHandler(domain string) func(dns.ResponseWriter, *dns.Msg) {
	return func(w dns.ResponseWriter, r *dns.Msg) {
		q := r.Question[0]

		log.Printf("Question: Type=%s Class=%s Name=%s\n", dns.TypeToString[q.Qtype], dns.ClassToString[q.Qclass], q.Name)

		if q.Qtype != dns.TypeA || q.Qclass != dns.ClassINET {
			handleNotFound(w, r)
			return
		}

		var query []string = strings.Split(
			strings.TrimSuffix(q.Name, fmt.Sprintf("%s.", domain)),
			".")

		if len(query) < 2 {
			log.Println("Query should contain both a hostname and a deployment name.")
			return
		}

		var hostname string = query[0]
		var deployment string = strings.Join(query[1:], ".")

		ip, err := nixopsHostIp(deployment, hostname)
		if err != nil {
			log.Println(err)
			handleNotFound(w, r)
			return
		}

		m := new(dns.Msg)
		m.SetReply(r)
		a := new(dns.A)
		a.Hdr = dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 30}
		a.A = ip
		m.Answer = []dns.RR{a}
		w.WriteMsg(m)
	}
}

func handleNotFound(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Rcode = dns.RcodeNameError // NXDOMAIN
	w.WriteMsg(m)
}

func main() {
	var addr = flag.String("addr", "127.0.0.1:5300", "listen address")
	var domain = flag.String("domain", "",
		"fake domain name to strip from requests e.g. host.ops -> host if -domain=.ops")

	flag.Parse()

	nixopsStateDb = openNixopsStateDb()
	defer nixopsStateDb.Close()

	server := &dns.Server{Addr: *addr, Net: "udp"}
	server.Handler = dns.HandlerFunc(domainHandler(*domain))

	log.Printf("Listening on %s\n", *addr)
	log.Fatal(server.ListenAndServe())
}
