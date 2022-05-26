# tiwatch
Watch API at scale, using TiDB as backend

#Usage

```Go
func main() {
	flag.Parse()
	dsn := "root:@tcp(localhost:4000)/test"
	w := tiwatch.New(dsn, "default")
	err := w.Init()
	if err != nil {
		panic(err)
	}

	ch := w.Watch("hello")

	if !*watchOnly {
		go func() {
			for {
				log.I("SEND")
				w.Set("hello", time.Now().String())
				time.Sleep(time.Second)

				if rand.Intn(2) == 0 {
					log.I("DEL")
					w.Delete("hello")
				}
			}
		}()
	}

	for {
		select {
		case m := <-ch:
			log.Infof("RECV: %s", m)
		}
	}
```
