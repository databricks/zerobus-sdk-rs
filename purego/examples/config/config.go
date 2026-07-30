// Package config holds the shared helpers the pure-Go Zerobus SDK examples use
// to read their connection settings from the environment.
//
// Every connection setting is read from the environment, so no value is ever
// baked into source — the same five variable names the C++, TypeScript, Java,
// and Python SDK examples use:
//
//	ZEROBUS_SERVER_ENDPOINT   Zerobus gRPC endpoint
//	DATABRICKS_WORKSPACE_URL  Unity Catalog / workspace URL (OAuth token exchange)
//	ZEROBUS_TABLE_NAME        target table, "catalog.schema.table"
//	DATABRICKS_CLIENT_ID      OAuth 2.0 client id
//	DATABRICKS_CLIENT_SECRET  OAuth 2.0 client secret
//
// The examples target an `orders` table; see the examples README for the
// CREATE TABLE statement.
package config

import (
	"fmt"
	"os"
)

// Settings is the resolved connection configuration for an example.
type Settings struct {
	ServerEndpoint string
	WorkspaceURL   string
	TableName      string
	ClientID       string
	ClientSecret   string
}

// RequireEnv reads a required environment variable, exiting with a clear
// message (status 2) if it is unset or empty. Exiting — rather than returning an
// error — keeps a misconfigured environment distinct from a genuine SDK error.
func RequireEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		fmt.Fprintf(os.Stderr,
			"error: environment variable %s is not set.\n"+
				"See the examples README for the required variables.\n", name)
		os.Exit(2)
	}
	return v
}

// Load reads the five standard connection variables from the environment,
// exiting via RequireEnv if any is missing.
func Load() Settings {
	return Settings{
		ServerEndpoint: RequireEnv("ZEROBUS_SERVER_ENDPOINT"),
		WorkspaceURL:   RequireEnv("DATABRICKS_WORKSPACE_URL"),
		TableName:      RequireEnv("ZEROBUS_TABLE_NAME"),
		ClientID:       RequireEnv("DATABRICKS_CLIENT_ID"),
		ClientSecret:   RequireEnv("DATABRICKS_CLIENT_SECRET"),
	}
}
