package account

import (
	"context"
	"errors"
	"time"

	"git.sr.ht/~rjarry/aerc/app"
	"git.sr.ht/~rjarry/aerc/commands"
)

type Settle struct {
	Timeout time.Duration `opt:"-t" desc:"Maximum wait time."`
	Account string        `opt:"-a" complete:"CompleteAccount" desc:"Wait for specified account instead of current."`
}

func init() {
	commands.Register(Settle{})
}

func (Settle) Description() string {
	return "Wait until all pending message list operations are complete."
}

func (Settle) Context() commands.CommandContext {
	return commands.MESSAGE_LIST
}

func (Settle) Aliases() []string {
	return []string{"settle"}
}

func (s *Settle) CompleteAccount(arg string) []string {
	return commands.FilterList(app.AccountNames(), arg, commands.QuoteSpace)
}

func (s Settle) Execute([]string) error {
	var acct *app.AccountView
	var err error

	if s.Account != "" {
		acct, err = app.Account(s.Account)
		if err != nil {
			return err
		}
	} else {
		acct = app.SelectedAccount()
		if acct == nil {
			return errors.New("No account selected")
		}
	}
	ctx := context.Background()
	if s.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.Timeout)
		defer cancel()
	}
	return acct.Settle(ctx)
}
