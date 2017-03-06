package main

import (
	"os"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("bkp")

func init() {
	format := logging.MustStringFormatter(`%{color}%{time:15:04:05.000} - %{shortfunc} [%{level:.1s}] ▶ %{color:reset} %{message}`)
	backend := logging.AddModuleLevel(logging.NewBackendFormatter(logging.NewLogBackend(os.Stderr, "", 0), format))
	backend.SetLevel(logging.DEBUG, "")
	logging.SetBackend(backend)
}

func main() {
	log.Debugf("debug %s", "hallo")
	log.Info("info")
	log.Notice("notice")
	log.Warning("warning")
	log.Error("err")
	log.Critical("crit")
}
