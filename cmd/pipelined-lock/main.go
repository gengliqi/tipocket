// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"time"

	// use mysql
	_ "github.com/go-sql-driver/mysql"

	test_infra "github.com/pingcap/tipocket/pkg/test-infra"

	pipelinedlock "github.com/pingcap/tipocket/tests/pipelined-lock"

	"github.com/pingcap/tipocket/cmd/util"
	"github.com/pingcap/tipocket/pkg/cluster"
	"github.com/pingcap/tipocket/pkg/control"
	"github.com/pingcap/tipocket/pkg/test-infra/fixture"
)

var (
	interval    = flag.Duration("interval", 2*time.Second, "check interval")
	concurrency = flag.Int("concurrency", 200, "concurrency of worker")
	NumValue    = flag.Int("numvalue", 100000, "num of value")
)

func main() {
	flag.Parse()
	cfg := control.Config{
		Mode:        control.ModeSelfScheduled,
		ClientCount: 1,
		RunTime:     fixture.Context.RunTime,
		RunRound:    1,
	}
	//kvs := []string{"127.0.0.1:20160", "127.0.0.1:20162", "127.0.0.1:20161"}
	suit := util.Suit{
		Config:      &cfg,
		Provisioner: cluster.NewK8sProvisioner(),
		//Provisioner: cluster.NewLocalClusterProvisioner([]string{"172.16.4.157:4000"}, []string{"172.16.4.157:2379"}, kvs),
		ClientCreator: pipelinedlock.CaseCreator{Cfg: &pipelinedlock.Config{
			Concurrency: *concurrency,
			Interval:    *interval,
		}},
		NemesisGens: util.ParseNemesisGenerators("shuffle-leader-scheduler,shuffle-region-scheduler,random-merge-scheduler"),
		ClusterDefs: test_infra.NewDefaultCluster(fixture.Context.Namespace, fixture.Context.Namespace,
			fixture.Context.TiDBClusterConfig),
	}
	suit.Run(context.Background())
}
