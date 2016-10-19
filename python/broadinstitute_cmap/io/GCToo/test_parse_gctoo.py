import unittest
import logging
import setup_GCToo_logger as setup_logger
import os
import pandas as pd
import numpy as np
import GCToo as GCToo
import parse_gctoo as pg
import write_gctoo as wg

FUNCTIONAL_TESTS_PATH = "functional_tests"

logger = logging.getLogger(setup_logger.LOGGER_NAME)


class TestParseGCToo(unittest.TestCase):
    def test_read_version_and_dims(self):
        version = "1.3"
        dims = ["10", "15", "3", "4"]
        fname = "testing_testing"

        f = open(fname, "wb")
        f.write(("#" + version + "\n"))
        f.write((dims[0] + "\t" + dims[1] + "\t" + dims[2] + "\t" + dims[3] + "\n"))
        f.close()

        (actual_version, n_rows, n_cols, n_rhd, n_chd) = pg.read_version_and_dims(fname)
        self.assertEqual(actual_version, version)
        self.assertEqual(n_rows, int(dims[0]))
        self.assertEqual(n_chd, int(dims[3]))

        # Remove the file I created
        os.remove(fname)

    def test_parse_into_3_df(self):
        gct_filepath = os.path.join(FUNCTIONAL_TESTS_PATH, "test_l1000.gct")
        e_dims = [978, 377, 11, 35]
        (row_metadata, col_metadata, data) = pg.parse_into_3_df(
            gct_filepath, e_dims[0], e_dims[1], e_dims[2], e_dims[3], None)

        # Check shapes of outputs
        self.assertTrue(row_metadata.shape == (e_dims[0], e_dims[2]),
                        ("row_metadata.shape = {} " +
                         "but expected it to be ({}, {})").format(row_metadata.shape,
                                                                  e_dims[0], e_dims[2]))
        self.assertTrue(col_metadata.shape == (e_dims[1], e_dims[3]),
                        ("col_metadata.shape = {} " +
                         "but expected it to be ({}, {})").format(col_metadata.shape,
                                                                  e_dims[1], e_dims[3]))
        self.assertTrue(data.shape == (e_dims[0], e_dims[1]),
                        ("data.shape = {} " +
                         "but expected it to be ({}, {})").format(data.shape,
                                                                  e_dims[0], e_dims[1]))

        # Type-check the data
        self.assertTrue(isinstance(data.iloc[0, 0], float), "The data should be a float, not a string.")

        # Check a few values
        correct_val = 11.3819
        self.assertTrue(data.iloc[0, 0] == correct_val,
                        ("The first value in the data matrix should be " +
                         str(correct_val) + " not {}").format(data.iloc[0, 0]))
        correct_val = 5.1256
        self.assertTrue(data.iloc[e_dims[0] - 1, e_dims[1] - 1] == correct_val,
                        ("The last value in the data matrix should be " +
                         str(correct_val) + " not {}").format(data.iloc[e_dims[0] - 1, e_dims[1] - 1]))
        correct_str = "LUA-4000"
        self.assertTrue(row_metadata.iloc[2, 3] == correct_str,
                        ("The 3rd row, 4th column of the row metadata should be " +
                         correct_str + " not {}").format(row_metadata.iloc[2, 3]))
        correct_str = 57
        self.assertTrue(col_metadata.iloc[e_dims[1] - 1, 0] == correct_str,
                        ("The last value in the first column of column metadata should be " +
                         str(correct_str) + " not {}").format(col_metadata.iloc[e_dims[1] - 1, 0]))

        # Check headers
        correct_str = "LJP005_A375_24H_X1_B19:P24"
        self.assertTrue(col_metadata.index.values[e_dims[1] - 1] == correct_str,
                        ("The last column metadata index should be " +
                         correct_str + " not {}").format(col_metadata.index.values[e_dims[1] - 1]))
        correct_str = "bead_batch"
        self.assertTrue(list(col_metadata)[3] == correct_str,
                        ("The fourth column metadata index value should be " +
                         correct_str + " not {}").format(list(col_metadata)[3]))
        correct_str = "203897_at"
        self.assertTrue(row_metadata.index.values[e_dims[0] - 1] == correct_str,
                        ("The last row metadata index value should be " + correct_str +
                         " not {}").format(row_metadata.index.values[e_dims[0] - 1]))
        self.assertTrue(data.index.values[e_dims[0] - 1] == correct_str,
                        ("The last data index value should be " + correct_str +
                         " not {}").format(data.index.values[e_dims[0] - 1]))

    def test_assemble_row_metadata(self):
        full_df = pd.DataFrame(
            [["id", "rhd1", "id", "cid1", "cid2"],
             ["chd1", "", "", "a", "b"],
             ["chd2", "", "", "55", "61"],
             ["chd3", "", "", "nah", "nope"],
             ["rid1", "C", "1.0", "0.3", "0.2"],
             ["rid2", "D", "2.0", np.nan, "0.9"]])
        full_df_dims = [2, 2, 2, 3]
        row_index = pd.Index(["rid1", "rid2"], name="rid")
        col_index = pd.Index(["rhd1", "id"], name="rhd")
        e_row_df = pd.DataFrame([["C", 1.0], ["D", 2.0]],
                                index = row_index,
                                columns = col_index)
        row_df = pg.assemble_row_metadata(full_df, full_df_dims[3],
                                          full_df_dims[0], full_df_dims[2])
        self.assertTrue(row_df.equals(e_row_df), (
            "\nrow_df:\n{}\ne_row_df:\n{}").format(row_df, e_row_df))

    def test_assemble_col_metadata(self):
        full_df = pd.DataFrame(
            [["id", "rhd1", "rhd2", "cid1", "cid2"],
             ["chd1", "", "", "a", "b"],
             ["chd2", "", "", "50", "60"],
             ["chd3", "", "", "1.0", np.nan],
             ["rid1", "C", "D", "0.3", "0.2"],
             ["rid2", "1.0", "2.0", np.nan, "0.9"]])
        full_df_dims = [2, 2, 2, 3]
        e_col_df = pd.DataFrame([["a", 50, 1.0], ["b", 60, np.nan]],
                                index=["cid1", "cid2"],
                                columns=["chd1", "chd2", "chd3"])
        col_df = pg.assemble_col_metadata(full_df, full_df_dims[3],
                                          full_df_dims[2], full_df_dims[1])
        self.assertTrue(col_df.equals(e_col_df))

    def test_assemble_data(self):
        full_df = pd.DataFrame(
            [["id", "rhd1", "rhd2", "cid1", "cid2"],
             ["chd1", "", "", "a", "b"],
             ["chd2", "", "", "55", "61"],
             ["chd3", "", "", "nah", "nope"],
             ["rid1", "C", "D", "0.3", "0.2"],
             ["rid2", "1.0", "2.0", np.nan, "0.9"]])
        full_df_dims = [2, 2, 2, 3]
        e_data_df = pd.DataFrame([[0.3, 0.2], [np.nan, 0.9]],
                                 index=["rid1", "rid2"],
                                 columns=["cid1", "cid2"])
        data_df = pg.assemble_data(full_df, full_df_dims[3], full_df_dims[0],
                                   full_df_dims[2], full_df_dims[1])
        self.assertTrue(data_df.equals(e_data_df))

    def test_parse(self):
        # L1000 gct
        l1000_file_path = os.path.join(FUNCTIONAL_TESTS_PATH, "test_l1000.gct")
        l1000_gct = pg.parse(l1000_file_path)

        # Check a few values
        correct_val = 11.3819
        self.assertTrue(l1000_gct.data_df.iloc[0, 0] == correct_val,
                        ("The first value in the data matrix should be " +
                         "{} not {}").format(str(correct_val), l1000_gct.data_df.iloc[0, 0]))
        correct_val = 58
        self.assertTrue(l1000_gct.col_metadata_df.iloc[0, 0] == correct_val,
                        ("The first value in the column metadata should be " +
                         "{} not {}").format(str(correct_val), l1000_gct.col_metadata_df.iloc[0, 0]))
        correct_val = "Analyte 11"
        self.assertTrue(l1000_gct.row_metadata_df.iloc[0, 0] == correct_val,
                        ("The first value in the row metadata should be " +
                         "{} not {}").format(str(correct_val), l1000_gct.row_metadata_df.iloc[0, 0]))

        # P100 gct
        p100_file_path = os.path.join(FUNCTIONAL_TESTS_PATH, "test_p100.gct")
        p100_gct = pg.parse(p100_file_path)

        # Check a few values
        correct_val = 0.918157217057044
        self.assertTrue(p100_gct.data_df.iloc[0, 0] == correct_val,
                        ("The first value in the data matrix should be " +
                         "{} not {}").format(str(correct_val), p100_gct.data_df.iloc[0, 0]))
        correct_val = "MCF7"
        self.assertTrue(p100_gct.col_metadata_df.iloc[0, 0] == correct_val,
                        ("The first value in the column metadata should be " +
                         "{} not {}").format(str(correct_val), p100_gct.col_metadata_df.iloc[0, 0]))
        correct_val = 1859
        self.assertTrue(p100_gct.row_metadata_df.iloc[0, 0] == correct_val,
                        ("The first value in the row metadata should be " +
                         "{} not {}").format(str(correct_val), p100_gct.row_metadata_df.iloc[0, 0]))


if __name__ == "__main__":
    setup_logger.setup(verbose=True)
    unittest.main()
