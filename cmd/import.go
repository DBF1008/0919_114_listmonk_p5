package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/knadh/listmonk/internal/auth"
	"github.com/knadh/listmonk/internal/subimporter"
	"github.com/knadh/listmonk/models"
	"github.com/labstack/echo/v4"
)

// ImportSubscribers handles the uploading and bulk importing of
// a ZIP file of one or more CSV files.
func (a *App) ImportSubscribers(c echo.Context) error {
	// Is an import already running?
	if a.importer.GetStats().Status == subimporter.StatusImporting {
		return echo.NewHTTPError(http.StatusBadRequest, a.i18n.T("import.alreadyRunning"))
	}

	// Unmarshal the JSON params.
	var opt subimporter.SessionOpt
	if err := json.Unmarshal([]byte(c.FormValue("params")), &opt); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest,
			a.i18n.Ts("import.invalidParams", "error", err.Error()))
	}

	// Filter list IDs against the current user's permitted lists.
	// Blocklist mode doesn't require list subscriptions.
	user := auth.GetUser(c)
	opt.ListIDs = user.FilterListsByPerm(auth.PermTypeManage, opt.ListIDs)
	if len(opt.ListIDs) == 0 && opt.Mode != subimporter.ModeBlocklist {
		return echo.NewHTTPError(http.StatusForbidden,
			a.i18n.Ts("globals.messages.permissionDenied", "name", "lists"))
	}

	// Validate mode.
	if opt.Mode != subimporter.ModeSubscribe && opt.Mode != subimporter.ModeBlocklist {
		return echo.NewHTTPError(http.StatusBadRequest, a.i18n.T("import.invalidMode"))
	}

	// If no status is specified, pick a default one.
	if opt.SubStatus == "" {
		switch opt.Mode {
		case subimporter.ModeSubscribe:
			opt.SubStatus = models.SubscriptionStatusUnconfirmed
		case subimporter.ModeBlocklist:
			opt.SubStatus = models.SubscriptionStatusUnsubscribed
		}
	}

	if opt.SubStatus != models.SubscriptionStatusUnconfirmed &&
		opt.SubStatus != models.SubscriptionStatusConfirmed &&
		opt.SubStatus != models.SubscriptionStatusUnsubscribed {
		return echo.NewHTTPError(http.StatusBadRequest, a.i18n.T("import.invalidSubStatus"))
	}

	if len(opt.Delim) != 1 {
		return echo.NewHTTPError(http.StatusBadRequest, a.i18n.T("import.invalidDelim"))
	}

	// Open the HTTP file.
	file, err := c.FormFile("file")
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest,
			a.i18n.Ts("import.invalidFile", "error", err.Error()))
	}

	src, err := file.Open()
	if err != nil {
		return err
	}

	// Start the importer session.
	opt.Filename = file.Filename
	sess, err := a.importer.NewSession(opt)
	if err != nil {
		src.Close()
		return echo.NewHTTPError(http.StatusInternalServerError,
			a.i18n.Ts("import.errorStarting", "error", err.Error()))
	}
	go sess.Start()

	// Capture the multipart form so the uploaded temp file (if any) can be
	// cleaned up once the import goroutine is done reading it. The echo
	// context itself must not be touched from the goroutine as it's
	// recycled after the handler returns.
	mpForm := c.Request().MultipartForm

	if strings.HasSuffix(strings.ToLower(file.Filename), ".csv") {
		// Stream the uploaded CSV straight into the importer without
		// copying it to disk first. The goroutine takes ownership of src.
		go func() {
			defer mpForm.RemoveAll()
			defer src.Close()
			if err := sess.LoadCSV(src, file.Size, rune(opt.Delim[0])); err != nil {
				lo.Printf("error importing CSV '%s': %v", file.Filename, err)
			}
		}()
	} else {
		// ZIP handling needs random access (the central directory is at
		// the end of the file), so the compressed upload is copied to a
		// temp file. The CSV inside is streamed out of the ZIP without
		// being extracted to disk, and the temp ZIP is deleted afterwards.
		out, err := os.CreateTemp("", "listmonk")
		if err != nil {
			src.Close()
			return echo.NewHTTPError(http.StatusInternalServerError,
				a.i18n.Ts("import.errorCopyingFile", "error", err.Error()))
		}

		if _, err = io.Copy(out, src); err != nil {
			out.Close()
			os.Remove(out.Name())
			src.Close()
			return echo.NewHTTPError(http.StatusInternalServerError,
				a.i18n.Ts("import.errorCopyingFile", "error", err.Error()))
		}
		src.Close()
		out.Close()
		mpForm.RemoveAll()

		go func() {
			if err := sess.LoadZIP(out.Name(), rune(opt.Delim[0])); err != nil {
				lo.Printf("error importing ZIP '%s': %v", file.Filename, err)
			}
		}()
	}

	return c.JSON(http.StatusOK, okResp{a.importer.GetStats()})
}

// GetImportSubscribers returns import statistics.
func (a *App) GetImportSubscribers(c echo.Context) error {
	s := a.importer.GetStats()
	return c.JSON(http.StatusOK, okResp{s})
}

// GetImportSubscriberStats returns import statistics.
func (a *App) GetImportSubscriberStats(c echo.Context) error {
	return c.JSON(http.StatusOK, okResp{string(a.importer.GetLogs())})
}

// StopImportSubscribers sends a stop signal to the importer.
// If there's an ongoing import, it'll be stopped, and if an import
// is finished, it's state is cleared.
func (a *App) StopImportSubscribers(c echo.Context) error {
	a.importer.Stop()
	return c.JSON(http.StatusOK, okResp{a.importer.GetStats()})
}
