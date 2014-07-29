import graphlab as gl
from graphlab.data_structures.sgraph import SGraph as _SGraph
import graphlab.aggregate as _Aggregate
from graphlab import SArray
from graphlab import SFrame
from graphlab import Vertex
from graphlab import SGraph
from graphlab import Edge 

g = gl.load_sgraph('/home/tweninge/wiki.graph')  
  
def initVertex(g):
    g.vertices['dist'] = 8888

    g.vertices['sent'] = 0
    #g.vertices['from_last_art'] = 0
    #g.vertices['count'] =0 
    g.vertices['isDead'] = 0
    #g.vertices['vid_set'] = SArray.from_const({}, g.summary()['num_vertices'])
#seen here have two function, for the cat, it is used to remember the articles, for art, it is used as the vid_set
#in fact, it is a dict with the form of {'id':[dist, from_last_art]}
    g.vertices['seen'] = SArray.from_const({}, g.summary()['num_vertices'])
    #g.vertices['msg_q'] = SArray.from_const([], g.summary()['num_vertices']) we donnot need it any more 

#g = gl.load_graph('/Users/liuzuozhu/Downloads/web-Google.txt', format='snap')   

initVertex(g)
#print g.get_vertices()

# def initEdge(g):  
#     #g.edges.head(5) 
#     g.edges['weight'] = 1 
#     if g.edges['__src_vid'].type==0 and g.edges['__dst_vid'].type ==0:
#         g.edges['weight'] = 8888
# initEdge(g)        
#get_nb has done the gather work, now we are going to the apply and scatter
#apply is to update the dist in article and some other is cat, scatter is to send mesg   
def  get_neighbors_fn(src, edge, dst):
    if src['type'] == 0 and dst['type'] == 0:
        temp = SArray([{src['id']:[8888,0]}])
        dst['seen'].append(temp)
                
def gas_apply_fn(src, edge, dst):
#initialize to trigger the graph
#I am not sure whether I can set the dist as 0, I didnot try now
    if src['id'] == 303:
        src['dist'] = 0  
#the apply of the graph     
    if src['sent'] == 1:
        src['isDead'] = 1
    elif src['type']== 0 and dst['type'] == 14: 
#if it is an article, isDead means been reached, this is executed in parallel
        if src['dist'] < 8888:
            src['sent'] = 1
#     elif dst['type']==  14: #if is a category, we add the src_art to the seen(), avoid receive msg from the same art
# #         if src['src_art'] not in dst['seen']:  
# #             dst['seen'].append(([src['src_art']]))
#         for i in range(0,src['seen'].size()):
#             if src['seen'][i] not in dst['seen']: #this will be a little slow, is there any function to get the union
#                 dst['seen'].append(([src['seen'][i]]))
            
#the scatter of the graph 
       
#from article to category
    if src['type'] == 0 and dst['type'] ==14 and src['isDead']==0:
        if src['dist'] != 8888: #make sure the single source
            temp = SArray([{src['id']: [src['dist']+1, src['from_last_art']+1]}])
            dst['seen'].append(temp)
        
#if category to category        
    elif src['type'] == 14 and dst['type'] ==14 and src['dist'] != 8888:
#for each edge, the dst cat should contain the src_art of the src  
         
        keys = src['seen'].dict_keys()  #will return a list of the key, and key is a list,the result is as [[],[]]
        for i in range(0,keys.size()):
            #dict_has_all_keys will return a list, so we use list[i] to get the value 1 or 0
            if  dst['seen'].dict_has_all_keys(keys[i][0])[0] == 0: 
                temp = SArray([{keys[i][0]: [src['seen'][i].get(keys[i][0])[0]+1, src['seen'][i].get(keys[i][0])[1]+1]}])
                dst['seen'].append(temp)
                          
#if category to article
#here, src['seen'].dict_keys() returns [[],[],[]], which is list of list
#we are going to find whether the src_art in the src exit in the dst, if so, compare and update it 
#src['seen'] returnns a sarray
#src['seen'][t] returns a dict of {key : [a , b]}
#keys_src[i][0] is an interger, equals to the value of the certain key
#dict.get(key), this function is used to get the value of the certain key, and here the value is as[dist, from_last_art]
    elif src['type'] == 14 and dst['type'] ==0 and src['dist'] != 8888 and dst['isDead'] ==0:      
        keys_src = src['seen'].dict_keys()
        keys_dst = dst['seen'].dict_keys() 
        values_dist = []
        for i in range(0,keys_src.size()): 
            for t in range(0, keys_dst.size()):
                if keys_dst[t][0] == keys_src[i][0]:
                    if src['seen'][i].get(keys_dst[i][0])[0] > dst['seen'][t].get(keys_dst[t][0])[0] : #if is the neighbor, then update the dist of neighbor              
                        dst['seen'][t].update({keys_dst[t][0]: [src['seen'][i].get(keys_src[i][0])[0]+1, src['seen'][i].get(keys[i][0])[1]+1]})
        values = dst['seen'].dict_values()
#values has the form of [[],[],[]], is a list of list
        for i in range (0,values.size()):
            values_dist[i] = values[i][0]
        dst['dist'] = min(values_dist)
        
    return (src, edge, dst)


g2 = g.triple_apply(get_neighbors_fn, mutated_fields=['seen'])
g3 = g2.triple_apply(gas_apply_fn, mutated_fields=['dist','sent','isDead','seen'])               
               
        
        
            
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    



    
    