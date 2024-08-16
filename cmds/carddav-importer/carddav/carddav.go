package carddav

type CardDAVConfig struct {
	// Server holds the URL of the CardDAV server.
	Server string
	// AllowInsecure can be set to true to disable
	// TLS certificate checks.
	AllowInsecure bool
	// User is the username required for HTTP Basic
	// authentication.
	User string
	// Password is the password required for HTTP Basic
	// authentication.
	Password string
	// AddressBook is the name of the adressbook to use.
	// If left empty CIS tries to discover the default
	// address book of the authenticated user.
	AddressBook string
}
