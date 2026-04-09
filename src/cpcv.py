"""
src/cpcv.py
Combinatorial Purged Cross-Validation Engine.
Implements memory purging and embargoing for financial time-series.
"""
import numpy as np
import pandas as pd
from itertools import combinations

class PurgedKFold:
    """
    Splits a time-series dataframe into Combinatorial Purged/Embargoed sets.
    """
    def __init__(self, n_splits: int = 6, n_test_splits: int = 2, purge_window: int = 0, embargo_window: int = 0):
        """
        :param n_splits: Total number of chronological blocks to slice the data into.
        :param n_test_splits: Number of blocks to use for testing in each combination.
        :param purge_window: Number of bars to delete BEFORE a test set (prevents memory leakage).
        :param embargo_window: Number of bars to delete AFTER a test set (prevents post-event learning).
        """
        self.n_splits = n_splits
        self.n_test_splits = n_test_splits
        self.purge_window = purge_window
        self.embargo_window = embargo_window

    def split(self, df: pd.DataFrame):
        """
        Generates the Train/Test index arrays for every valid combinatorial path.
        Yields (train_indices, test_indices)
        """
        n_samples = len(df)
        indices = np.arange(n_samples)
        
        # 1. Divide the indices into 'n_splits' chronological blocks
        split_indices = np.array_split(indices, self.n_splits)
        
        # 2. Generate all combinations of test blocks
        test_block_combinations = list(combinations(range(self.n_splits), self.n_test_splits))
        
        for test_blocks in test_block_combinations:
            test_indices = []
            train_indices = []
            
            # Extract the actual index numbers for the test blocks
            for i in test_blocks:
                test_indices.extend(split_indices[i])
            
            # Build the training set while applying Purge and Embargo
            for i in range(self.n_splits):
                if i in test_blocks:
                    continue # Skip test blocks
                
                block_indices = split_indices[i].copy()
                
                # Check relationship with all test blocks to apply Purge/Embargo
                for test_i in test_blocks:
                    # If this Train block immediately PRECEDES a Test block -> PURGE the end of Train
                    if i == test_i - 1:
                        if self.purge_window > 0 and len(block_indices) > self.purge_window:
                            block_indices = block_indices[:-self.purge_window]
                            
                    # If this Train block immediately FOLLOWS a Test block -> EMBARGO the start of Train
                    if i == test_i + 1:
                        if self.embargo_window > 0 and len(block_indices) > self.embargo_window:
                            block_indices = block_indices[self.embargo_window:]
                
                train_indices.extend(block_indices)
                
            yield np.array(train_indices), np.array(test_indices)

    def get_n_splits(self):
        """Returns the total number of combinatorial paths generated."""
        return len(list(combinations(range(self.n_splits), self.n_test_splits)))