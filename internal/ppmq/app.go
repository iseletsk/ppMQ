package ppmq

import "github.com/benbjohnson/clock"

// Application structure holds app Config, Clock (so that we can set it to test clock on tests)
// as well as Test boolean flag to represent test runs
type Application struct {
	Clock clock.Clock
	Conf  Config
	Test  bool
}

// App singleton representing the application config.
var App = Application{clock.NewMock(), Config{}, false}
