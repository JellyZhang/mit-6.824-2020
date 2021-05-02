package shardctrler

func getCopyConfig(cfg Config) Config {
	newCfg := Config{
		Num: cfg.Num + 1,
		Shards: [10]int{
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
		},
		Groups: map[int][]string{},
	}

	for i, target := range cfg.Shards {
		newCfg.Shards[i] = target
	}

	for gid, servers := range cfg.Groups {
		newCfg.Groups[gid] = servers
	}
	return newCfg
}

func reBalance(cfg *Config) {
	allGids := make([]int, 0)
	for gid, _ := range cfg.Groups {
		allGids = append(allGids, gid)
	}
	index := 0

	if len(allGids) > 0 {
		for i := range cfg.Shards {
			cfg.Shards[i] = allGids[index]
			index = (index + 1) % len(allGids)
		}
	}
}
