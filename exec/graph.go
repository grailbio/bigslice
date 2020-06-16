// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/grailbio/base/log"
)

func (s *Session) handleDebug(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("content-type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, debugIndexHtml)
}

var debugIndexHtml = `<!DOCTYPE html>
<meta charset="utf-8">
<head>
<title>
/debug
</title>
</head>
<body>

<dl>
<dt><a href="/debug/status">/debug/status</a></dt>
<dd>bigslice task and machine status</dd>
<dt><a href="/debug/tasks">/debug/tasks</a></dt>
<dd>bigslice task graph</dd>
<dt><a href="/debug/trace">/debug/trace</a></dt>
<dd>Chrome-compatible event trace</dd>
</dl>
</body>
</html>
`

func (s *Session) handleTasksGraph(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	roots := make(map[*Task]bool, len(s.roots))
	for task := range s.roots {
		roots[task] = true
	}
	s.mu.Unlock()
	tasks := make(map[*Task]bool, 2*len(roots))
	for task := range roots {
		task.all(tasks)
	}

	indexed := make(map[*Task]int, len(tasks))
	for task := range tasks {
		indexed[task] = len(indexed)
	}

	type node struct {
		Name   string `json:"name"`
		Group  int    `json:"group"`
		Radius int    `json:"radius"`
	}
	type link struct {
		Source int `json:"source"`
		Target int `json:"target"`
	}

	var graph struct {
		Nodes []node `json:"nodes"`
		Links []link `json:"links"`
	}

	graph.Nodes = make([]node, len(tasks))
	for task, index := range indexed {
		var node node
		node.Name = task.Name.String()
		if roots[task] {
			node.Radius = 10
		} else {
			node.Radius = 5
		}
		node.Group = int(task.State())
		graph.Nodes[index] = node
		for _, dep := range task.Deps {
			for i := 0; i < dep.NumTask(); i++ {
				deptask := dep.Task(i)
				graph.Links = append(graph.Links, link{index, indexed[deptask]})
			}
		}
	}
	w.Header().Add("content-type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(graph); err != nil {
		log.Error.Printf("Session.handleTasksGraph: json.Encode: %v", err)
		http.Error(w, err.Error(), 500)
	}
}

func (s *Session) handleTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("content-type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, tasksGraphHtml)
}

var tasksGraphHtml = `<!DOCTYPE html>
<meta charset="utf-8">
<style>

.links line {
  stroke: #999;
  stroke-opacity: 0.6;
}

.nodes circle {
  stroke: #fff;
  stroke-width: 1.5px;
}

text {
  font-family: sans-serif;
  font-size: 10px;
}

</style>
<svg width="960" height="600"></svg>
<script src="https://d3js.org/d3.v4.min.js"></script>
<script>

var svg = d3.select("svg"),
    width = +svg.attr("width"),
    height = +svg.attr("height");

var color = d3.scaleOrdinal(d3.schemeCategory20);

var simulation = d3.forceSimulation()
    .force("charge", d3.forceManyBody().strength(-600))
    .force("link", d3.forceLink())
    .force("center", d3.forceCenter(width / 2, height / 2));

d3.json("/debug/tasks/graph", function(error, graph) {
  if (error) throw error;

  var link = svg.append("g")
    .attr("class", "links")
    .selectAll("line")
    .data(graph.links)
    .enter().append("line")

  var node = svg.append("g")
    .attr("class", "nodes")
    .selectAll("g")
    .data(graph.nodes)
    .enter().append("g")

  node.append("circle")
      .attr("r", function(d) { return d.radius })
      .attr("fill", function(d) { return color(d.group); })
      .call(d3.drag()
          .on("start", dragstarted)
          .on("drag", dragged)
          .on("end", dragended));

  node.append("text")
      .text(function(d) {
        return d.name;
      })
      .attr('x', 6)
      .attr('y', 3);

  node.append("title")
      .text(function(d) { return d.name; });

  simulation
      .nodes(graph.nodes)
      .on("tick", ticked);

  simulation.force("link")
      .links(graph.links);

  function ticked() {
    link
        .attr("x1", function(d) { return d.source.x; })
        .attr("y1", function(d) { return d.source.y; })
        .attr("x2", function(d) { return d.target.x; })
        .attr("y2", function(d) { return d.target.y; });

    node
        .attr("transform", function(d) {
          return "translate(" + d.x + "," + d.y + ")";
        })
  }
});

function dragstarted(d) {
  if (!d3.event.active) simulation.alphaTarget(0.3).restart();
  d.fx = d.x;
  d.fy = d.y;
}

function dragged(d) {
  d.fx = d3.event.x;
  d.fy = d3.event.y;
}

function dragended(d) {
  if (!d3.event.active) simulation.alphaTarget(0);
  d.fx = null;
  d.fy = null;
}

</script>

`
