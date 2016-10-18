import unittest
import pandas as pd
import numpy as np
import GCToo
import logging
import setup_GCToo_logger


logger = logging.getLogger(setup_GCToo_logger.LOGGER_NAME)

class TestGCToo(unittest.TestCase):
    def test_slice_bool(self):
        data = pd.DataFrame([[1,2,3],[4,5,6],[7,8,9],[10,11,12]],
                            index=['a','b','c','d'],
                            columns=['e','f','g'])
        row_meta = pd.DataFrame([['rm1','rm2'],['rm3','rm4'],
                                 ['rm5','rm6'],['rm7','rm8']],
                                index=['a','b','c','d'],
                                columns=['row_field1', 'row_field2'])
        col_meta = pd.DataFrame([['cm1','cm2'],['cm3','cm4'],['cm5','cm6']],
                                index=['e','f','g'],
                                columns=['col_field1','col_field2'])
        my_gctoo = GCToo.GCToo(data_df=data,
                               row_metadata_df=row_meta,
                               col_metadata_df=col_meta)

        row_bool = [True, False, True, True]
        col_bool = [True, True, False]
        out_gctoo = GCToo.slice(my_gctoo, row_bool, col_bool)

        self.assertEqual(out_gctoo.data_df.shape, (3, 2),
                         'Size of returned data_df is wrong: {}'.format(out_gctoo.data_df.shape))
        self.assertTrue(np.array_equal(out_gctoo.data_df.index.values, np.array(['a','c','d'])),
                         'Incorrect data rows returned: {}'.format(out_gctoo.data_df.index.values))
        self.assertTrue(np.array_equal(out_gctoo.col_metadata_df.index.values, np.array(['e','f'])),
                         'Incorrect column metadata rows returned: {}'.format(out_gctoo.col_metadata_df.index.values))

        # TODO(lev): test edge case of only one row or sample remaining

    def test_assemble_multi_index_df(self):

        # TODO: Add test of only row ids present as metadata
        # TODO: Add test of only col ids present as metadata 

        g = GCToo.GCToo()
        
        g.row_metadata_df = pd.DataFrame({"a":range(3)}, index=range(4,7))
        logger.debug("g.row_metadata_df:  {}".format(g.row_metadata_df))

        g.col_metadata_df = pd.DataFrame({"b":range(7,10)}, index=range(10,13))
        logger.debug("g.col_metadata_df:  {}".format(g.col_metadata_df))

        g.data_df = pd.DataFrame({10:range(13,16), 11:range(16,19), 12:range(19,22)}, index=range(4,7))
        logger.debug("g.data_df:  {}".format(g.data_df))

        g.assemble_multi_index_df()
        logger.debug("g.multi_index_df:  {}".format(g.multi_index_df))

        assert "a" in g.multi_index_df.index.names, g.multi_index_df.index.names
        assert "rid" in g.multi_index_df.index.names, g.multi_index_df.index.names

        assert "b" in g.multi_index_df.columns.names, g.multi_index_df.columns.names
        assert "cid" in g.multi_index_df.columns.names, g.multi_index_df.columns.names

        r = g.multi_index_df.xs(7, level="b", axis=1)
        logger.debug("r:  {}".format(r))
        assert r.xs(4, level="rid", axis=0).values[0][0] == 13, r.xs(4, level="rid", axis=0).values[0][0]
        assert r.xs(5, level="rid", axis=0).values[0][0] == 14, r.xs(5, level="rid", axis=0).values[0][0]
        assert r.xs(6, level="rid", axis=0).values[0][0] == 15, r.xs(6, level="rid", axis=0).values[0][0]

    def test_init(self):
        # Create test data
        data_df = pd.DataFrame([[1, 2, 3], [4, 5, 6]],
                               index=["A", "B"], columns=["a", "b", "c"])
        row_metadata_df = pd.DataFrame([["rhd_A", "rhd_B"], ["rhd_C", "rhd_D"]],
                                       index=["A", "B"], columns=["rhd1", "rhd2"])
        col_metadata_df = pd.DataFrame(["chd_a", "chd_b", "chd_c"],
                                       index=["a", "b", "c"], columns=["chd1"])

        # happy path
        GCToo.GCToo(data_df=data_df, row_metadata_df=row_metadata_df,
                    col_metadata_df=col_metadata_df)

    def test_check_uniqueness(self):
        not_unique_data_df = pd.DataFrame([[1, 2, 3], [4, 5, 6]],
                                          index=["A", "B"], columns=["a", "b", "a"])
        not_unique_rhd = pd.DataFrame([["rhd_A", "rhd_B"], ["rhd_C", "rhd_D"]],
                                       index=["A", "B"], columns=["rhd1", "rhd1"])

        # cids in data_df are not unique
        with self.assertRaises(AssertionError) as e:
            GCToo.GCToo(data_df=not_unique_data_df)
        self.assertIn("'a' 'b' 'a'", str(e.exception))

        # rhds are not unique in row_metadata_df
        with self.assertRaises(AssertionError) as e:
            GCToo.GCToo(row_metadata_df=not_unique_rhd)
        self.assertIn("'rhd1' 'rhd1'", str(e.exception))

    def test_rid_consistency_check(self):
        data_df = pd.DataFrame([[1, 2, 3], [4, 5, 6]],
                               index=["A", "B"], columns=["a", "b", "c"])
        inconsistent_rids = pd.DataFrame([["rhd_A", "rhd_B"], ["rhd_C", "rhd_D"]],
                                         index=["A", "C"], columns=["rhd1", "rhd2"])
        with self.assertRaises(AssertionError) as e:
            GCToo.GCToo.rid_consistency_check(GCToo.GCToo(
                data_df=data_df, row_metadata_df=inconsistent_rids))
        self.assertIn("The rids are inconsistent", str(e.exception))

    def test_cid_consistency_check(self):
        data_df = pd.DataFrame([[1, 2, 3], [4, 5, 6]],
                               index=["A", "B"], columns=["a", "b", "c"])
        inconsistent_cids = pd.DataFrame(["chd_a", "chd_b", "chd_c"],
                                         index=["a", "b", "C"], columns=["chd1"])
        with self.assertRaises(AssertionError) as e:
            GCToo.GCToo.cid_consistency_check(GCToo.GCToo(
                data_df=data_df, col_metadata_df=inconsistent_cids))
        self.assertIn("The cids are inconsistent", str(e.exception))

    def test_multi_index_df_to_component_dfs(self):
        mi_df_index = pd.MultiIndex.from_arrays(
            [["D", "E"], [-666, -666], ["dd", "ee"]],
            names=["rid", "rhd1", "rhd2"])
        mi_df_columns = pd.MultiIndex.from_arrays(
            [["A", "B", "C"], [1, 2, 3], ["Z", "Y", "X"]],
            names=["cid", "chd1", "chd2"])
        mi_df = pd.DataFrame(
            [[1, 3, 5], [7, 11, 13]],
            index=mi_df_index, columns=mi_df_columns)

        e_row_metadata_df = pd.DataFrame(
            [[-666, "dd"], [-666, "ee"]],
            index=pd.Index(["D", "E"], name="rid"),
            columns=pd.Index(["rhd1", "rhd2"], name="rhd"))
        e_col_metadata_df = pd.DataFrame(
            [[1, "Z"], [2, "Y"], [3, "X"]],
            index=pd.Index(["A", "B", "C"], name="cid"),
            columns=pd.Index(["chd1", "chd2"], name="chd"))
        e_data_df = pd.DataFrame(
            [[1, 3, 5], [7, 11, 13]],
            index=pd.Index(["D", "E"], name="rid"),
            columns=pd.Index(["A", "B", "C"], name="cid"))

        (data_df, row_df, col_df) = GCToo.multi_index_df_to_component_dfs(mi_df)

        self.assertTrue(col_df.equals(e_col_metadata_df))
        self.assertTrue(row_df.equals(e_row_metadata_df))
        self.assertTrue(data_df.equals(e_data_df))

if __name__ == "__main__":
    setup_GCToo_logger.setup(verbose=True)

    unittest.main()

