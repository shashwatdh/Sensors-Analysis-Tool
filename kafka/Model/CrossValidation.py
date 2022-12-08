import numpy as np
import pandas as pd
from sklearn.model_selection import KFold, StratifiedKFold, GroupKFold

class K_Fold_CV:
   
    n_splits = 5
    shuffle = False 
    random_state = None
    
    def __init__(self, n_folds, shuffle, random_state, data_x, data_y):
        self.n_splits = n_folds
        self.shuffle = bool(shuffle)
        self.random_state = None if random_state == 'None' else random_state
        self.data_X = data_x
        self.data_Y = data_y
        
    def K_Fold(self):
        kf = KFold(n_splits = self.n_splits,
                       shuffle = self.shuffle,
                           random_state= self.random_state)
        
        data_splits_indices = []
        data_splits = []
        for train_index, test_index in kf.split(self.data_X):
            #data_splits_indices.append((train_index, test_index))
            X_train = self.data_X.iloc[train_index, :]
            X_test = self.data_X.iloc[test_index, :]
            Y_train = self.data_Y.iloc[train_index]
            Y_test = self.data_Y.iloc[test_index]
            
            data_splits.append((X_train, X_test, Y_train, Y_test))
        
        return data_splits
        

class Stratified_K_Fold_CV:
    
    n_splits = 5
    shuffle = False 
    random_state = None
    
    def __init__(self, n_folds, shuffle, random_state, data_x, data_y):
        self.n_splits = n_folds
        self.shuffle = shuffle
        self.random_state = random_state
        self.data_X = data_x
        self.data_Y = data_y
        
    def SK_Fold(self):
        
        skf = StratifiedKFold(n_splits = self.n_splits,
                                  shuffle = self.shuffle,
                                      random_state= self.random_state)
        
        data_splits_indices = []
        data_splits = []
        for train_index, test_index in skf.split(self.data_X):
            #data_splits_indices.append((train_index, test_index))
            X_train = self.data_X.iloc[train_index, :]
            X_test = self.data_X.iloc[test_index, :]
            Y_train = self.data_Y.iloc[train_index]
            Y_test = self.data_Y.iloc[test_index]
            
            data_splits.append((X_train, X_test, Y_train, Y_test))
        
        return data_splits
    
class Group_K_Fold_CV:
    
    n_splits = 5
    
    def __init__(self, n_splits, x_train, y_train, grp_labels):
        self.n_splits = n_splits
        self.X = x_train
        self.Y = y_train
        self.grp_labels = grp_labels
        
    def Group_K_Fold(self):
        gkf = GroupKFold(n_splits=self.n_splits)
        
        data_splits_indices = []
        data_splits = []
        grp_ids = []
        for train_index, test_index in gkf.split(self.X, self.Y, self.grp_labels):
            #data_splits_indices.append((train_index, test_index))
            X_train = self.X.iloc[train_index, :]
            X_test = self.X.iloc[test_index, :]
            Y_train = self.Y.iloc[train_index]
            Y_test = self.Y.iloc[test_index]
            
            grp_ids.append(self.grp_labels[test_index[0]])
            data_splits.append((X_train, X_test, Y_train, Y_test))
        
        return data_splits, grp_ids
    
class Stratified_GRP_K_Fold_CV:
    n_splits = 5
    shuffle = False 
    random_state = None
    
    def __init__(self, n_folds, shuffle, random_state, data_x, data_y):
        self.n_splits = n_folds
        self.shuffle = shuffle
        self.random_state = random_state
        self.data_X = data_x
        self.data_Y = data_y
        
    def SGK_Fold(self):
        
        skf = StratifiedKFold(n_splits = self.n_splits,
                                  shuffle = self.shuffle,
                                      random_state= self.random_state)
        
        data_splits_indices = []
        data_splits = []
        for train_index, test_index in skf.split(self.data_X):
            #data_splits_indices.append((train_index, test_index))
            X_train = self.data_X.iloc[train_index, :]
            X_test = self.data_X.iloc[test_index, :]
            Y_train = self.data_Y.iloc[train_index]
            Y_test = self.data_Y.iloc[test_index]
            
            data_splits.append((X_train, X_test, Y_train, Y_test))
        
        return data_splits