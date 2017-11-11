
library(readr)
library(igraph)

EntradaR3 <-read_delim("C:/tendencias/r4.csv", ",", escape_double = FALSE, trim_ws = TRUE)

grafo <- graph.data.frame(EntradaR3[1:29,])

plot(grafo,layout=layout.fruchterman.reingold, vertex.size=8, vertex.label.dist=1, vertex.label=NA, vertex.color="red", edge.arrow.size=1, edge.color="blue")


pr.tmp <- data.frame(pr.value= page.rank(grafo)$vector)
pr     <- data.frame(pr.desc=row.names(pr.tmp), pr.val = pr.tmp[,1])

head(pr[order(pr$pr.val,decreasing = T),])

V(grafo)$label.cex =  0.6 + pr$pr.val*3 
plot(grafo,layout=layout.fruchterman.reingold, vertex.size=pr$pr.val*100, vertex.label=NA, vertex.label.dist=0.4, vertex.color="red", edge.arrow.size=0.3)
