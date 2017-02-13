function add_category(json,dict_country){
  function find_index(curr_json,country){
    var country_children = curr_json["children"]
    for (var i=0; i < country_children.length;i++){
      if (country_children[i]["name"] == country){
        return i
      }
    }
    return -1
  }

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
        key_obj["size"] = 20
        event_type["children"].push(key_obj)
      }
      time_lst.push(event_type)
    }
    return time_lst
  }

  function add_child(insertname,bool,tot){
    var new_add = {}
    new_add["children"] = []
    if (bool == true){
      new_add["name"] = insertname + " => " + tot
    }
    else{
      new_add["name"] = insertname
    }
    return new_add
  }

  var country = dict_country["Country"]
  var timeframe = dict_country["Timeframe"]
  var date = dict_country["Date"]
  var total = dict_country["Total"]
  var info = dict_country["Overview"]
  var country_ind = find_index(json,country) //returns index of country
  if (country_ind == -1){
    var country_dict = add_child(country) //here  {"name":Ecuador,"children":[]}
    var add_date = add_child(date,true,total) //here {"name":"20150709","children":[]}
    add_date["children"] = event_children(info)
    country_dict["children"].push(add_date)
    json["children"].push(country_dict)
    return json
  }
  else{
        var new_date = add_child(date,true,total) //{"name":date,children:[]}
        new_date["children"] = event_children(info)
        json["children"][country_ind]["children"].push(new_date)
        return json
  }
}

function add_simple(json,dict_country){

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
                   poot = add_simple(poot,result["result"])
		   console.log(poot)
		   createCircles(poot);
                }
              });
             })


var svg = d3.select("svg"),
  margin = 20,
  diameter = +svg.attr("width"),
  g = svg.append("g").attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");

var color = d3.scaleLinear()
  .domain([-1, 5])
  .range(["hsl(152,80%,80%)", "hsl(228,30%,40%)"])
  .interpolate(d3.interpolateHcl);

var pack = d3.pack()
  .size([diameter - margin, diameter - margin])
  .padding(2);

function createCircles(root){
root = d3.hierarchy(root)
    .sum(function(d) { return d.size; })
    .sort(function(a, b) { return b.value - a.value; });

var focus = root,
    nodes = pack(root).descendants(),
    view;

var circle = g.selectAll("circle")
  .data(nodes)
  .enter().append("circle")
    .attr("class", function(d) { return d.parent ? d.children ? "node" : "node node--leaf" : "node node--root"; })
    .style("fill", function(d) { return d.children ? color(d.depth) : null; })
    .on("click", function(d) { if (focus !== d) zoom(d), d3.event.stopPropagation(); });

var text = g.selectAll("text")
  .data(nodes)
  .enter().append("text")
    .attr("class", "label")
    .style("fill-opacity", function(d) { return d.parent === root ? 1 : 0; })
    .style("display", function(d) { return d.parent === root ? "inline" : "none"; })
    .text(function(d) { return d.data.name; });

var node = g.selectAll("circle,text");

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
//});
