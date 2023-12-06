
//! This file contains notes on the Delta stepping algorithm from the Bale project.



// This is well studied problem.

// It is used in classes on algorithms, data structures, and graph theory. It is also used as a benchmark in Grap
// h500 benchmark suite. We are using this problem to evaluate what we have learned about our programming models.
//  Our approach is that of someone given the assignment to get an SSSP application running reasonable well, at s
// cale, using our programming models.


// We consider three algorithms: Dijsktra's, Delta-Stepping, and Bellman-Ford. Good references for these uses are
//  easily found online. The explanations given here are meant to explain the code and our understanding of the p
// arallelism, not to explain the algorithms. To study the Delta-Stepping algorithm we are using the original Mey
// er/Sanders paper, delta-stepping algorithm.


// These algorithm work by assign a tentative weight, tent(v), to each vertex. Initially the weight of the given 
// vertex is zero and all other weights are set to infinity. The algorithms proceed by changing the state of the 
// vertices from unreached (with infinite weight) to unsettled (with a tentative weight) to settled (meaning that
//  the lightest weight path to that vertex is known). The vertices change state via the process of relaxing edge
// s. Relaxing an edge (v,w) replaces tent(w) with the min(tent(w), tent(v)+c(v,w)).


// There is subtle relationship between the length of a path (the number of edge in the path) and the weight of t
// he path. Clearly, it is possible to have a longer path that is lighter than a shorter path. If a path from v0 
// to w contains the vertex v, then the weight of the path from v0 to v is less than the weight of the path from 
// v0 thru v to w. If v is an intermediate vertex on a lightest path from v0 to w, then that path from 

//  to 

//  is also a lightest path. The different algorithms play off these relationships in different ways.


// Dijsktra's algorithm works by considering the weight tent(v) of the "unsettled" vertices. One proves that ligh
// test such vertex does, in fact, have the correct weight. So, that vertex can be "settled" and we need only rel
// ax edges from that vertex. Furthermore, we never have to relax those edges again. This gives the most efficien
// t algorithm in the sense that edges are relaxed exactly once. The problem of finding the lightest unsettled ve
// rtex is addressed below.


// Bellman-Ford relaxes edges based on the length of paths to the "unsettled" and "unreached" vertices. Basically
//  the algorithm simply relaxes all of the edges in the graph over and over again until none of the tentative we
// ights tent(v) change. The algorithm finds the lightest tentative weights to all vertices on paths of length i,
//  for i equal zero to the length of a longest path.


// The Meyer/Sanders delta-stepping algorithm uses ideas from both of the previous algorithms. Unsettled vertices
//  are kept in "buckets" based on their by tent(v) weights; bucket i contains vertices with tent(v) at least i*d
// elta and less than (i+1)*delta, where delta is a parameter. The active bucket is the ith bucket (with the smal
// lest i) that has unsettled vertices. Edges in the graph are considered light if their weight is less than or e
// qual to delta and heavy otherwise. The algorithm "settles" the vertices in the active bucket with a Bellman-Fo
// rd approach using only the light edges. Then the algorithm relaxes the vertices that were in the active bucket
//  using the heavy edges. This uses an extension of Dijsktra's approach because the heavy edges cannot put verti
// ces into the active bucket. The efficiency of the algorithm comes at the price of flow control and data-struct
// ure manipulations to maintain the buckets. Here is psuedo code for the algorithm:



// ======================================================================


// // We write an edge with tail v, head w and weight (cost) as {v,w,c}.

// // We will say the weight of the lightest tentative path 

// // to a vertex is the price of the vertex.  The relax routine is 

// // more involved than it is in other sssp algorithms.  In addition 

// // to (possibly) reducing the price of the head of an edge,

// // it can move the head from one bucket to another, possibly to the 

// // bucket of the tail.


// def relax(w, p, B):

//   if tent[w] > p:

//     remove.bucket(w, B(tent[w]/delta))

//     add.bucket(w, B(p/delta))

//     tent[w] = x



// program sssp:


//   set tent[v] = inf for all v


//   relax(s, 0, B)  // sets tent[s] = 0  and puts s in B[0]


//   while "there is a non-empty bucket" :

//     let B[i] "be the first (smallest i) non-empty bucket"

//     set R = NULL   // set of vertices that we will retire


//     while B[i] is not empty:

//       let v = a vertex in B[i]

//       for all light edges {v,w,c}:

//         p = tent[v] + c  // possible new price of w

//         relax (w, p, B)  // possibly adding w to B[i]

//         add v to R

//         remove v from B[i]

    

//     for all v in R:

//       for all heavy edges {v,w,c}

//         p = tent[v] + c  // possible new price of w

//         relax (w, p, B)  // won't add w to B[i]
