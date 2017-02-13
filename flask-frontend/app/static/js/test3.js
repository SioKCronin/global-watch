

var svg = d3.select("svg"),
     margin = 20,
     diameter = +svg.attr("width"),
     g = svg.append("g").attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");

  var format = d3.format(",d")

  var color = d3.scaleLinear()
      .domain([-1, 5])
      .range(["hsl(152,80%,80%)", "hsl(228,30%,40%)"])
      .interpolate(d3.interpolateHcl);

  var pack = d3.pack()
      .size([diameter - margin, diameter - margin])
      .padding(2);


var poot = {"name":"flare","children":[]}
$( "#target" ).click(function() {
             var data = {};
             data.country = $("#country").val();
             data.timeframe = $("#timeframe").val();
             data.eventdate = $("#eventdate").val();
             $.ajax({
                type : "POST",
                url : "/overview",
                data: JSON.stringify(data, null, '\t'),
                contentType: 'application/json;charset=UTF-8',
                success: function(result) {
                   //poot = add_simple(poot,result["result"])
                   draw(poot,result["result"]);
                  // poot = getData(poot,result["result"])
                }
              });
             })

function draw(root,updates_dict){
  //var root = getData(root,updates_dict)
  //console.log("This is the root: ",root)

  //STRATIFU MIGHT HAVE TO GO IN HERE??

  function getData(json,dict_country){
    function event_children(event_dict){ //returns a list with event children
    var time_lst = []
    var event_names = Object.keys(event_dict)
    for (var i = 0; i<event_names.length; i++){
      var event_type = {}
      event_type["name"] = event_names[i]
      event_type["children"] = []
      event_info = event_dict[event_names[i]]
      for (var key in event_info){
        key_obj = {}
        key_obj["name"] = key + " => " + event_info[key]
        key_obj["size"] = 2000
        event_type["children"].push(key_obj)
      }
      time_lst.push(event_type)
    }
    return time_lst
    }

    var country = dict_country["Country"]
    var timeframe = dict_country["Timeframe"]
    var date = dict_country["Date"]
    var total = dict_country["Total"]
    var info = dict_country["Overview"]
    json["children"].push({"name":country,"children":event_children(info)})
    return json

  }

  var root = getData(root,updates_dict)

  root = d3.hierarchy(root)
      .sum(function(d) { return d.size; })
      .sort(function(a, b) { return b.value - a.value; });

  var focus = root,
      nodes = pack(root).descendants(),
      view;

  var circle = g.selectAll("circle")
    .data(nodes,function(d){
      return d.country; //I thinks this will fail
    })

  circle.exit().remove()

  circle = circle.enter()
      .append("circle")
      .attr("class", function(d) { return d.parent ? d.children ? "node" : "node node--leaf" : "node node--root"; })
      .style("fill", function(d) { return d.children ? color(d.depth) : null; })
      .on("click", function(d) { if (focus !== d) zoom(d), d3.event.stopPropagation(); })
      .merge(circle);


  var text = g.selectAll("text")
    .data(nodes,function(d){
      return d.country;
    })
  
  text.exit().remove();

  text = text.enter().append("text")
      .attr("class", "label")
      .merge(text)

  text = text.style("fill-opacity", function(d) { return d.parent === root ? 1 : 0; })
      .style("display", function(d) { return d.parent === root ? "inline" : "none"; })
      .text(function(d) { return d.data.name; });
  
  var node = g.selectAll("circle,text");

  node.append("title")
    .text(function(d){
      return d.country
    })

  svg
      .style("background", color(-1))
      .on("click", function() { zoom(root); });

  zoomTo([root.x, root.y, root.r * 2 + margin]);

  function zoom(d) {
    var focus0 = focus; focus = d;

    var transition = d3.transition()
        .duration(d3.event.altKey ? 7500 : 750)
        .tween("zoom", function(d) {
          var i = d3.interpolateZoom(view, [focus.x, focus.y, focus.r * 2 + margin]);
          return function(t) { zoomTo(i(t)); };
        });

    transition.selectAll("text")
      .filter(function(d) { return d.parent === focus || this.style.display === "inline"; })
        .style("fill-opacity", function(d) { return d.parent === focus ? 1 : 0; })
        .on("start", function(d) { if (d.parent === focus) this.style.display = "inline"; })
        .on("end", function(d) { if (d.parent !== focus) this.style.display = "none"; });
  }

  function zoomTo(v) {
    var k = diameter / v[2]; view = v;
    node.attr("transform", function(d) { return "translate(" + (d.x - v[0]) * k + "," + (d.y - v[1]) * k + ")"; });
    circle.attr("r", function(d) { return d.r * k; });
  }

}
