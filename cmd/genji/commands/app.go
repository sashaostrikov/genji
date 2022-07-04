package commands

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/genjidb/genji/cmd/genji/dbutil"
	"github.com/genjidb/genji/cmd/genji/shell"
	"github.com/urfave/cli/v2"
)

// NewApp creates the Genji CLI app.
func NewApp() *cli.App {
	app := cli.NewApp()
	app.Name = "Genji"
	app.Usage = "Shell for the Genji database"
	app.EnableBashCompletion = true

	app.Commands = []*cli.Command{
		NewInsertCommand(),
		NewVersionCommand(),
		NewDumpCommand(),
		NewRestoreCommand(),
		NewBenchCommand(),
	}

	// inject cancelable context to all commands (except the shell command)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer cancel()
		<-ch
	}()

	for i := range app.Commands {
		action := app.Commands[i].Action
		app.Commands[i].Action = func(c *cli.Context) error {
			c.Context = ctx
			return action(c)
		}
	}

	// Root command
	app.Action = func(c *cli.Context) error {
		dbPath := c.Args().First()
		// Support shell restarts when shell.ErrRestartShell is returned
		for {
			err := MainAction(c.Context, dbPath)
			restartShellError, ok := err.(*shell.RestartShellError)
			// if no error is returned or the error is not a RestartShellError, then interrupt the action
			if !ok || err == nil {
				return err
			}
			// in case of restarting the shell we need to point to right database directory
			dbPath = restartShellError.DbPath
		}
	}

	app.After = func(c *cli.Context) error {
		cancel()
		return nil
	}

	return app
}

func MainAction(ctx context.Context, dbPath string) error {
	if dbutil.CanReadFromStandardInput() {
		db, err := dbutil.OpenDB(ctx, dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		return dbutil.ExecSQL(ctx, db, os.Stdin, os.Stdout)
	}

	return shell.Run(ctx, &shell.Options{
		DBPath: dbPath,
	})
}
