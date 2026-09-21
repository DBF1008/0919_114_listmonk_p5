package main

import (
	"archive/zip"
	"encoding/json"
	"io"
	"net/http"
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
		_ = src.Close()
		return echo.NewHTTPError(http.StatusInternalServerError,
			a.i18n.Ts("import.errorStarting", "error", err.Error()))
	}
	go sess.Start()

	if strings.HasSuffix(strings.ToLower(file.Filename), ".csv") {
		// Stream the upload directly. Count the lines first and rewind; the
		// multipart upload is backed by a seekable file (in-memory or the
		// server's multipart temp spool), so no extra temporary copy is needed.
		numLines, err := subimporter.CountLines(src)
		if err != nil {
			_ = src.Close()
			return echo.NewHTTPError(http.StatusInternalServerError,
				a.i18n.Ts("import.errorCopyingFile", "error", err.Error()))
		}
		if _, err := src.Seek(0, io.SeekStart); err != nil {
			_ = src.Close()
			return echo.NewHTTPError(http.StatusInternalServerError,
				a.i18n.Ts("import.errorCopyingFile", "error", err.Error()))
		}

		// The stream is consumed asynchronously, so it is closed here only
		// once the import is done (instead of via defer at handler return).
		go func() {
			defer src.Close()
			if err := sess.LoadCSV(src, numLines, rune(opt.Delim[0])); err != nil {
				a.log.Printf("error importing CSV '%s': %v", file.Filename, err)
			}
		}()
	} else {
		// Only 1 CSV from the ZIP is considered. If multiple files have
		// to be processed, counting the net number of lines (to track progress),
		// keeping the global import state (failed / successful) etc. across
		// multiple files becomes complex. Instead, it's just easier for the
		// end user to concat multiple CSVs (if there are multiple in the first)
		// place and upload as one in the first place.
		//
		// The ZIP is read straight from the upload stream (multipart uploads
		// implement io.ReaderAt), without extracting it to disk. The CSV entry
		// is decompressed twice: once to count lines and once to import.
		zr, err := zip.NewReader(src, file.Size)
		if err != nil {
			_ = src.Close()
			return echo.NewHTTPError(http.StatusInternalServerError,
				a.i18n.Ts("import.errorProcessingZIP", "error", err.Error()))
		}

		// Find the first .csv entry.
		var zf *zip.File
		for _, f := range zr.File {
			if f.FileInfo().IsDir() {
				continue
			}
			if !strings.HasSuffix(strings.ToLower(f.FileInfo().Name()), ".csv") {
				continue
			}
			zf = f
			break
		}
		if zf == nil {
			_ = src.Close()
			return echo.NewHTTPError(http.StatusInternalServerError, "no CSV files found in the ZIP")
		}

		// First decompression pass: count the lines for progress tracking.
		rc, err := zf.Open()
		if err != nil {
			_ = src.Close()
			return echo.NewHTTPError(http.StatusInternalServerError,
				a.i18n.Ts("import.errorProcessingZIP", "error", err.Error()))
		}
		numLines, err := subimporter.CountLines(rc)
		_ = rc.Close()
		if err != nil {
			_ = src.Close()
			return echo.NewHTTPError(http.StatusInternalServerError,
				a.i18n.Ts("import.errorProcessingZIP", "error", err.Error()))
		}

		// Second decompression pass: the actual import.
		rc, err = zf.Open()
		if err != nil {
			_ = src.Close()
			return echo.NewHTTPError(http.StatusInternalServerError,
				a.i18n.Ts("import.errorProcessingZIP", "error", err.Error()))
		}

		go func() {
			defer src.Close()
			defer rc.Close()
			if err := sess.LoadCSV(rc, numLines, rune(opt.Delim[0])); err != nil {
				a.log.Printf("error importing ZIP '%s': %v", file.Filename, err)
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
