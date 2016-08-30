import os
import unittest
import logging
import numpy as np
import pandas as pd

import setup_GCToo_logger as setup_logger
import concat_gctoo as cg
import parse_gctoo as pg

logger = logging.getLogger(setup_logger.LOGGER_NAME)

FUNCTIONAL_TESTS_DIR = "functional_tests"

class TestConcatGCToo(unittest.TestCase):
    def test_left_right(self):
        # Verify that concatenation replicates the output file
        left_gct_path = os.path.join(FUNCTIONAL_TESTS_DIR, "test_merge_left.gct")
        right_gct_path = os.path.join(FUNCTIONAL_TESTS_DIR, "test_merge_right.gct")
        expected_gct_path = os.path.join(FUNCTIONAL_TESTS_DIR, "test_merged_left_right.gct")

        left_gct = pg.parse(left_gct_path)
        right_gct = pg.parse(right_gct_path)
        expected_gct = pg.parse(expected_gct_path)

        # Merge left and right
        concated_gct = cg.hstack([left_gct, right_gct], None, False)

        self.assertTrue(expected_gct.data_df.equals(concated_gct.data_df), (
            "\nconcated_gct.data_df:\n{}\nexpected_gct.data_df:\n{}".format(
                concated_gct.data_df, expected_gct.data_df)))
        self.assertTrue(expected_gct.row_metadata_df.equals(concated_gct.row_metadata_df))
        self.assertTrue(expected_gct.col_metadata_df.equals(concated_gct.col_metadata_df))

    def test_concat_row_meta(self):
        meta1 = pd.DataFrame(
            [["r1_1", "r1_2", "r1_3"],
            ["r2_1", "r2_2", "r2_3"],
            ["r3_1", "r3_2", "r3_3"]],
            index=["r1", "r2", "r3"],
            columns=["rhd1", "rhd2", "rhd3"])
        meta2 = pd.DataFrame(
            [["r1_1", "r1_2", "r1_3"],
            ["r2_1", "r2_2", "r2_3"],
            ["r3_1", "r3_2", "r3_33"]],
            index=["r1", "r2", "r3"],
            columns=["rhd1", "rhd2", "rhd3"])
        e_meta = pd.DataFrame(
            [["r1_1", "r1_2"],
            ["r2_1", "r2_2"],
            ["r3_1", "r3_2"]],
            index=["r1", "r2", "r3"],
            columns=["rhd1", "rhd2"])

        with self.assertRaises(AssertionError) as e:
            out_meta_df = cg.concat_row_meta([meta1, meta2], None)
        self.assertIn("rids are duplicated", str(e.exception))

        # happy path, using fields_to_remove
        out_meta_df = cg.concat_row_meta([meta1, meta2], fields_to_remove=["rhd3"])

        self.assertTrue(out_meta_df.equals(e_meta), (
            "\nout_meta_df:\n{}\ne_meta:\n{}".format(out_meta_df, e_meta)))

    def test_concat_col_meta(self):
        meta1 = pd.DataFrame(
            [["a", "b"], ["c", "d"]],
            index=["c1", "c2"],
            columns=["hd1", "hd2"])
        meta2 = pd.DataFrame(
            [["e", "f", "g"], ["h", "i", "j"]],
            index=["c2", "c3"],
            columns=["hd1", "hd2", "hd3"])
        e_concated = pd.DataFrame(
            [["a", "b", np.nan], ["c", "d", np.nan],
             ["e", "f", "g"], ["h", "i", "j"]],
            index=["c1", "c2", "c2", "c3"],
            columns=["hd1", "hd2", "hd3"])

        out_concated = cg.concat_col_meta([meta1, meta2])
        self.assertTrue(out_concated.equals(e_concated), (
            "\nout_concated:\n{}\ne_concated:\n{}".format(
                out_concated, e_concated)))

    def test_concat_data(self):
        df1 = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6]],
            index=["a", "b"],
            columns=["s1", "s2", "s3"])
        df2 = pd.DataFrame(
            [[7, 8, 9], [10, 11, 12]],
            index=["a", "b"],
            columns=["s4", "s5", "s6"])
        e_concated = pd.DataFrame(
            [[1, 2, 3, 7, 8, 9], [4, 5, 6, 10, 11, 12]],
            index=["a", "b"],
            columns=["s1", "s2", "s3", "s4", "s5", "s6"])

        out_concated = cg.concat_data([df1, df2])
        self.assertTrue(out_concated.equals(e_concated), (
            "\nout_concated:\n{}\ne_concated:\n{}".format(
                out_concated, e_concated)))

    def test_do_reset_sample_ids(self):
        col_df = pd.DataFrame(
            [[1, 2], [3, 4], [5, 6]],
            index=["s1", "s2", "s1"],
            columns=["hd1", "hd2"])
        data_df = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6]],
            index=["a", "b"],
            columns=["s1", "s2", "s1"])
        inconsistent_data_df = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6]],
            index=["a", "b"],
            columns=["s1", "s2", "s3"])
        e_col_df = pd.DataFrame(
            [["s1", 1, 2], ["s2", 3, 4], ["s1", 5, 6]],
            index=[0, 1, 2],
            columns=["old_cid", "hd1", "hd2"])
        e_data_df = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6]],
            index=["a", "b"],
            columns=[0, 1, 2])

        # Check the assert statement
        with self.assertRaises(AssertionError) as e:
            (_, _) = cg.do_reset_sample_ids(col_df, inconsistent_data_df)
        self.assertIn("do not agree", str(e.exception))

        # Happy path
        (out_col_df, out_data_df) = cg.do_reset_sample_ids(col_df, data_df)
        self.assertTrue(out_col_df.equals(e_col_df), (
            "\nout_col_df:\n:{}\ne_col_df:\n{}".format(out_col_df, e_col_df)))
        self.assertTrue(out_data_df.equals(e_data_df), (
            "\nout_data_df:\n:{}\ne_data_df:\n{}".format(out_data_df, e_data_df)))


if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()