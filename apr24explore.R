###see the model for first 10 lines
model <- xgb.dump(bst, with.stats = T)  #first 10 models
model[1:10]

# Get the feature real names
names <- dimnames(trainMatrix)[[2]]
# Compute feature importance matrix
importance_matrix <- xgb.importance(names, model = bst)

# Plot the tree, n_first_tree can be changed to 
xgb.plot.tree(feature_names = names, model = bst,n_first_tree = 10)

