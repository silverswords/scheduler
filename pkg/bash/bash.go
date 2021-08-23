package bash

// type Bash struct {
// 	cmd  *exec.Cmd
// 	envs []string
// }

// func New(commands ...string) (*Bash, error) {
// 	b := &Bash{
// 		cmd:   make([]*exec.Cmd, len(commands)),
// 		stdin: nil,
// 	}

// 	for _, command := range commands {
// 		cmd := exec.Command("bash", "-c", command)
// 		b.cmd = append(b.cmd, cmd)
// 	}

// 	stdin, err := cmd.StdinPipe()
// 	if err != nil {
// 		return nil, err
// 	}

// 	for _, env := range envs {
// 		_, err := stdin.Write([]byte(env))
// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	cmd.Stdout = os.Stdout
// 	cmd.Stderr = os.Stderr

// 	return &Bash{
// 		cmd:   cmd,
// 		stdin: stdin,
// 	}, nil
// }

// func (b *Bash) Exec(cmd string) error {
// 	b.cmd
// 	b.cmd.Run()
// 	return nil
// }
