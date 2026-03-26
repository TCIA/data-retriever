package main

import (
	"embed"
	"log"
	"os"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/logger"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/windows"
	"github.com/wailsapp/wails/v2/pkg/options/mac"
)

//go:embed frontend/dist
var assets embed.FS

func main() {
	// Create an instance of the app structure
	app := NewApp()

	// Windows and Linux pass the opened file as the first argument.
  // Store it before Wails starts so HandleFileOpen-equivalent logic works.
  if len(os.Args) > 1 {
      candidate := os.Args[1]
      app.pendingFileOpen = candidate
  }

	// Create application with options
	err := wails.Run(&options.App{
		Title:             "TCIA Data Retriever",
		Width:             650,
		Height:            500,
		MinWidth:          650,
		MaxWidth: 				 650,
		MinHeight:         500,
		DisableResize:     false,
		Fullscreen:        false,
		Frameless:         false,
		StartHidden:       false,
		HideWindowOnClose: false,
		Assets:            assets,
		LogLevel:          logger.DEBUG,
		OnStartup:         app.startup,
		OnShutdown:        app.shutdown,
		Bind: []interface{}{
			app,
		},
		// Windows platform specific options
		Windows: &windows.Options{
			WebviewIsTransparent: false,
			WindowIsTranslucent:  false,
			DisableWindowIcon:    false,
		},
		Mac: &mac.Options{
        OnFileOpen: func(filePath string) {
            app.HandleFileOpen(filePath)
        },
    },
	})

	if err != nil {
		log.Fatal(err)
	}
}
