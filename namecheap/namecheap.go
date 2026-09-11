package main

// The Namecheap XML API client: exactly the five calls the tools need.
// https://www.namecheap.com/support/api/methods/

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const apiBase = "https://api.namecheap.com/xml.response"

type ncClient struct {
	apiUser, apiKey, username, clientIP string
	hc                                  *http.Client
}

type apiResponse struct {
	Status string   `xml:"Status,attr"`
	Errors []ncErr  `xml:"Errors>Error"`
	Result struct { // union of the result shapes we read
		Checks []struct {
			Domain       string `xml:"Domain,attr"`
			Available    string `xml:"Available,attr"`
			Premium      string `xml:"IsPremiumName,attr"`
			PremiumPrice string `xml:"PremiumRegistrationPrice,attr"`
		} `xml:"DomainCheckResult"`
		Created struct {
			Domain     string `xml:"Domain,attr"`
			Registered string `xml:"Registered,attr"`
			Charged    string `xml:"ChargedAmount,attr"`
		} `xml:"DomainCreateResult"`
		DNSSet struct {
			Success string `xml:"IsSuccess,attr"`
		} `xml:"DomainDNSSetHostsResult"`
		DNSGet struct {
			Domain    string `xml:"Domain,attr"`
			EmailType string `xml:"EmailType,attr"` // MX | MXE | FWD | OX — setHosts resets this unless resent
			Hosts     []struct {
				Name    string `xml:"Name,attr"`
				Type    string `xml:"Type,attr"`
				Address string `xml:"Address,attr"`
				MXPref  string `xml:"MXPref,attr"`
				TTL     string `xml:"TTL,attr"`
			} `xml:"host"`
		} `xml:"DomainDNSGetHostsResult"`
		Pricing []struct {
			Name     string `xml:"Name,attr"` // the tld
			Products []struct {
				Name   string `xml:"Name,attr"` // "register"
				Prices []struct {
					Duration string `xml:"Duration,attr"`
					Price    string `xml:"Price,attr"`
					Currency string `xml:"Currency,attr"`
				} `xml:"Price"`
			} `xml:"Product"`
		} `xml:"UserGetPricingResult>ProductType>ProductCategory"`
	} `xml:"CommandResponse"`
}

type ncErr struct {
	Number string `xml:"Number,attr"`
	Text   string `xml:",chardata"`
}

func (c *ncClient) call(ctx context.Context, command string, params map[string]string) (*apiResponse, error) {
	q := url.Values{
		"ApiUser":  {c.apiUser},
		"ApiKey":   {c.apiKey},
		"UserName": {c.username},
		"ClientIp": {c.clientIP},
		"Command":  {command},
	}
	for k, v := range params {
		q.Set(k, v)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("namecheap: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var out apiResponse
	if err := xml.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("namecheap: unreadable response: %w", err)
	}
	if !strings.EqualFold(out.Status, "OK") {
		msgs := make([]string, 0, len(out.Errors))
		for _, e := range out.Errors {
			msgs = append(msgs, fmt.Sprintf("%s (%s)", strings.TrimSpace(e.Text), e.Number))
		}
		return nil, fmt.Errorf("namecheap %s: %s", command, strings.Join(msgs, "; "))
	}
	return &out, nil
}
