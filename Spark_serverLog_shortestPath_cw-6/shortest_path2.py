
from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def input_filter(single_rdd):
    if not single_rdd[0].isalpha():
        return single_rdd
def create_source_dest_pair(single_rdd):
    splitted = single_rdd.split(':')
    key = splitted[0]
    val_original = splitted[1]
    values_list = list(val_original.split(" "))
    values_list = list(filter(None,values_list))
    return (key, values_list)
def filter_out_node(single_edge_graph,bc_path):
    for path in list(bc_path.value):
        if path[0] == single_edge_graph[0]:
            return single_edge_graph      
def already_found(bc_path,node,source):
    for x in bc_path.value:
         if x[0] == node and x[1][0]== source:
             return True, x[1][1]
    return False,0
def create_path(single_edge_graph,bc_path,new_distance):         
    temp_list =[bc_path.value[0]]
    for x in list(bc_path.value):                
        if x[0] == single_edge_graph[0]:
            for node_edge in single_edge_graph[1]:
                is_already_found,old_distance = already_found(bc_path,node_edge,x[0])
                if is_already_found:
                    distance = old_distance
                else:
                    distance = new_distance         
                source =  x[0]               
                node =node_edge        
                single_path = (node,(source,distance))
                temp_list.append(single_path)
    return temp_list
def find_shortest_path(value1,value2):
    if value1[1] == min(value1[1],value2[1]):
        return (value1[0],min(value1[1],value2[1]))
    else: 
        return (value2[0],min(value1[1],value2[1]))
def create_output_text(single_path):
    if single_path[0] == single_path[1][0]:
        source = '-'
    else:
        source = 'source '+single_path[1][0]
    return ['node '+single_path[0]+": "+source +', distance '+str(single_path[1][1])]
def create_output_path(output_dict,source_node,dest_node):
    output_list =[dest_node]
    init = dest_node    
    while init != source_node:
        temp = output_dict[init][0]
        output_list.append(temp)
        init = temp    
    return output_list[::-1]

def main(inputs, output,source_node,dest_node):
    # main logic starts here
    if source_node == dest_node:
        assert False, ('Source node and Destination node must be different.')
    
    RDD_original = sc.textFile(inputs)
    rdd = RDD_original.filter(input_filter)
    edge_graph = rdd.map(create_source_dest_pair).cache()
    print(edge_graph.collect())
    
    initial_path = [(source_node,(source_node,0))]       
    path = sc.parallelize(initial_path)
    bc_path = sc.broadcast(path.collect())
    
    for i in range(1,6+1):                              
        path = edge_graph.filter(lambda x: filter_out_node(x,bc_path))
        
        print('--------- ', i ,' ------------------') 
        path = path.flatMap(lambda x: create_path(x,bc_path,i))
        print("initial path:\n",path.collect())
      
        path = path.reduceByKey(find_shortest_path)      
        #path = path.distinct()              
        path = path.sortBy(lambda a: a[1][1])
        print("path after reduced+sorted:\n", path.collect())
        
        output_text = path.map(create_output_text)        
        output_dict = path.collectAsMap()
        print('\n',output_text.collect())

    #    path.saveAsTextFile(output+'/iter-'+str(i))

        if path.filter(lambda x: x[0]==dest_node).collect() :
            break

        bc_path = sc.broadcast(path.collect())     
        print('\n')

    finalpath = create_output_path(output_dict,source_node,dest_node)
    print('\n',finalpath)
    finalpath= sc.parallelize(finalpath)
   # finalpath.saveAsTextFile(output + '/path')

if __name__ == '__main__':
    conf = SparkConf().setAppName('shortest path')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    source_node = sys.argv[3]
    dest_node = sys.argv[4] 
    main(inputs, output,source_node,dest_node)
