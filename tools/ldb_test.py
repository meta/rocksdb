#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import glob
import fcntl
import os
import os.path
import re
import shutil
import subprocess
import tempfile
import time
import unittest


def my_check_output(*popenargs, **kwargs):
    """
    If we had python 2.7, we should simply use subprocess.check_output.
    This is a stop-gap solution for python 2.6
    """
    if "stdout" in kwargs:
        raise ValueError("stdout argument not allowed, it will be overridden.")
    process = subprocess.Popen(
        stderr=subprocess.PIPE, stdout=subprocess.PIPE, *popenargs, **kwargs
    )
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise Exception("Exit code is not 0.  It is %d.  Command: %s" % (retcode, cmd))
    return output.decode("utf-8")


def run_err_null(cmd):
    return os.system(cmd + " 2>/dev/null ")


class LDBTestCase(unittest.TestCase):
    def setUp(self):
        self.TMP_DIR = tempfile.mkdtemp(prefix="ldb_test_")
        self.DB_NAME = "testdb"

    def tearDown(self):
        assert (
            self.TMP_DIR.strip() != "/"
            and self.TMP_DIR.strip() != "/tmp"
            and self.TMP_DIR.strip() != "/tmp/"
        )  # Just some paranoia

        shutil.rmtree(self.TMP_DIR)

    def dbParam(self, dbName):
        return "--db=%s" % os.path.join(self.TMP_DIR, dbName)

    def assertRunOKFull(
        self, params, expectedOutput, unexpected=False, isPattern=False
    ):
        """
        All command-line params must be specified.
        Allows full flexibility in testing; for example: missing db param.
        """
        output = my_check_output(
            './ldb %s |grep -v "Created bg thread"' % params, shell=True
        )
        if not unexpected:
            if isPattern:
                self.assertNotEqual(expectedOutput.search(output.strip()), None)
            else:
                self.assertEqual(output.strip(), expectedOutput.strip())
        else:
            if isPattern:
                self.assertEqual(expectedOutput.search(output.strip()), None)
            else:
                self.assertNotEqual(output.strip(), expectedOutput.strip())

    def assertRunFAILFull(self, params):
        """
        All command-line params must be specified.
        Allows full flexibility in testing; for example: missing db param.
        """
        try:
            my_check_output(
                './ldb %s >/dev/null 2>&1 |grep -v "Created bg thread"' % params,
                shell=True,
            )
        except Exception:
            return
        self.fail(
            "Exception should have been raised for command with params: %s" % params
        )

    def assertRunErrorFull(self, params):
        """Asserts that ldb itself, rather than an output filter, exits nonzero."""
        try:
            my_check_output("./ldb %s >/dev/null 2>&1" % params, shell=True)
        except Exception:
            return
        self.fail("ldb should have failed for command with params: %s" % params)

    def assertRunOK(self, params, expectedOutput, unexpected=False):
        """
        Uses the default test db.
        """
        self.assertRunOKFull(
            "{} {}".format(self.dbParam(self.DB_NAME), params),
            expectedOutput,
            unexpected,
        )

    def assertRunFAIL(self, params):
        """
        Uses the default test db.
        """
        self.assertRunFAILFull("{} {}".format(self.dbParam(self.DB_NAME), params))

    def assertRunError(self, params):
        """Uses the default test DB and requires a nonzero ldb exit status."""
        self.assertRunErrorFull("{} {}".format(self.dbParam(self.DB_NAME), params))

    def testSimpleStringPutGet(self):
        print("Running testSimpleStringPutGet...")
        self.assertRunFAIL("put x1 y1")
        self.assertRunOK("put --create_if_missing x1 y1", "OK")
        self.assertRunOK("get x1", "y1")
        self.assertRunFAIL("get x2")

        self.assertRunOK("put x2 y2", "OK")
        self.assertRunOK("get x1", "y1")
        self.assertRunOK("get x2", "y2")
        self.assertRunOK("multi_get x1 x2", "x1 ==> y1\nx2 ==> y2")
        self.assertRunOK("get x3", "Key not found")
        self.assertRunOK("multi_get x3", "Key not found: x3")

        self.assertRunFAIL("put_entity x4")
        self.assertRunFAIL("put_entity x4 cv1")
        self.assertRunOK("put_entity x4 :cv1", "OK")
        self.assertRunOK("get_entity x4", ":cv1")

        self.assertRunOK("put_entity x5 cn1:cv1 cn2:cv2", "OK")
        self.assertRunOK("get_entity x5", "cn1:cv1 cn2:cv2")

        self.assertRunOK(
            "scan --from=x1 --to=z",
            "x1 ==> y1\nx2 ==> y2\nx4 ==> cv1\nx5 ==> cn1:cv1 cn2:cv2",
        )
        self.assertRunOK("put x3 y3", "OK")

        self.assertRunOK(
            "scan --from=x1 --to=z",
            "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> cv1\nx5 ==> cn1:cv1 cn2:cv2",
        )
        self.assertRunOK(
            "scan",
            "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> cv1\nx5 ==> cn1:cv1 cn2:cv2",
        )
        self.assertRunOK(
            "scan --from=x",
            "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> cv1\nx5 ==> cn1:cv1 cn2:cv2",
        )

        self.assertRunOK("scan --to=x2", "x1 ==> y1")
        self.assertRunOK("scan --from=x1 --to=z --max_keys=1", "x1 ==> y1")
        self.assertRunOK("scan --from=x1 --to=z --max_keys=2", "x1 ==> y1\nx2 ==> y2")

        self.assertRunOK("delete x4", "OK")
        self.assertRunOK("delete x5", "OK")

        self.assertRunOK(
            "scan --from=x1 --to=z --max_keys=3", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3"
        )
        self.assertRunOK(
            "scan --from=x1 --to=z --max_keys=4", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3"
        )
        self.assertRunOK("scan --from=x1 --to=x2", "x1 ==> y1")
        self.assertRunOK("scan --from=x2 --to=x4", "x2 ==> y2\nx3 ==> y3")
        self.assertRunFAIL("scan --from=x4 --to=z")  # No results => FAIL
        self.assertRunFAIL("scan --from=x1 --to=z --max_keys=foo")

        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3")

        self.assertRunOK("delete x1", "OK")
        self.assertRunOK("scan", "x2 ==> y2\nx3 ==> y3")

        self.assertRunOK("delete NonExistentKey", "OK")
        # It is weird that GET and SCAN raise exception for
        # non-existent key, while delete does not

        self.assertRunOK("checkconsistency", "OK")

    def testRebindOptionsFileInManifest(self):
        self.assertRunOK("put --create_if_missing key value", "OK")
        db_path = os.path.join(self.TMP_DIR, self.DB_NAME)
        default_only_options = os.path.basename(
            max(glob.glob(os.path.join(db_path, "OPTIONS-*")))
        )
        with open(
            os.path.join(db_path, default_only_options), "r", encoding="utf-8"
        ) as options_file:
            default_only_contents = options_file.read()
        self.assertRunOK("create_column_family repair_extra", "OK")
        selected_options = os.path.basename(
            max(glob.glob(os.path.join(db_path, "OPTIONS-*")))
        )
        selected_options_path = os.path.join(db_path, selected_options)
        self.assertRunError(
            "update_manifest --rebind_options_file --options_file={}".format(
                selected_options
            )
        )
        with open(selected_options_path, "r+", encoding="utf-8") as options_file:
            options_contents = options_file.read()
            self.assertIn(
                "track_options_file_number_in_manifest=false", options_contents
            )
            options_contents = options_contents.replace(
                "track_options_file_number_in_manifest=false",
                "track_options_file_number_in_manifest=true",
            )
            options_file.seek(0)
            options_file.write(options_contents)
            options_file.truncate()
        with open(selected_options_path, "rb") as selected_options_file:
            selected_options_contents = selected_options_file.read()

        # Missing a live MANIFEST CF is unsafe even when the selected snapshot
        # enables tracking.
        missing_options = "OPTIONS-{}".format(
            int(selected_options.split("-")[1]) + 1000000
        )
        missing_options_path = os.path.join(db_path, missing_options)
        with open(missing_options_path, "w", encoding="utf-8") as options_file:
            options_file.write(
                default_only_contents.replace(
                    "track_options_file_number_in_manifest=false",
                    "track_options_file_number_in_manifest=true",
                )
            )
        self.assertRunError(
            "update_manifest --rebind_options_file --options_file={}".format(
                missing_options
            )
        )
        os.remove(missing_options_path)

        # A selected snapshot with an extra CF is the safe state left by a
        # partial batch. Drop that CF from MANIFEST while retaining this file.
        self.assertRunOK("drop_column_family repair_extra", "OK")
        with open(selected_options_path, "wb") as selected_options_file:
            selected_options_file.write(selected_options_contents)

        self.assertRunError("update_manifest --rebind_options_file")
        self.assertRunError(
            "update_manifest --rebind_options_file "
            "--options_file={} --update_temperatures".format(selected_options)
        )

        # The repair is an offline mutation and must reject a live writer.
        with open(os.path.join(db_path, "LOCK"), "a") as lock_file:
            fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertRunError(
                "update_manifest --rebind_options_file --options_file={}".format(
                    selected_options
                )
            )
            fcntl.lockf(lock_file, fcntl.LOCK_UN)

        # Repair must leave room for recovery plus caller-dependent writable
        # open allocations, including the maximum number of missing CFs.
        max_file_number = (1 << 62) - 1
        max_missing_column_families = (1 << 32) - 1
        writable_open_headroom = 3 * max_missing_column_families + 16
        exhausted_options = "OPTIONS-{}".format(
            max_file_number - writable_open_headroom
        )
        exhausted_options_path = os.path.join(db_path, exhausted_options)
        shutil.copyfile(
            os.path.join(db_path, selected_options), exhausted_options_path
        )
        self.assertRunError(
            "update_manifest --rebind_options_file --options_file={}".format(
                selected_options
            )
        )
        os.remove(exhausted_options_path)

        # A torn physical tail must be removed before the new edit is appended;
        # otherwise log replay stops before the repaired commit.
        with open(os.path.join(db_path, "CURRENT"), encoding="utf-8") as current:
            manifest_name = current.read().strip()
        manifest_path = os.path.join(db_path, manifest_name)
        with open(manifest_path, "ab") as manifest:
            manifest.write(b"\x01\x02\x03\x04")

        self.assertRunOK(
            "update_manifest --rebind_options_file --options_file={}".format(
                selected_options
            ),
            "Manifest updates successful",
        )
        rebound_options_path = max(
            glob.glob(os.path.join(db_path, "OPTIONS-*")),
            key=lambda path: int(os.path.basename(path).split("-")[1]),
        )
        rebound_options = os.path.basename(rebound_options_path)
        self.assertNotEqual(selected_options, rebound_options)
        with open(selected_options_path, "rb") as selected_options_file:
            self.assertEqual(selected_options_contents, selected_options_file.read())
        with open(rebound_options_path, "rb") as rebound_options_file:
            self.assertEqual(selected_options_contents, rebound_options_file.read())

        rebound_options_number = int(rebound_options.split("-")[1])
        scan_output = my_check_output(
            ["./ldb", self.dbParam(self.DB_NAME), "scan", "--try_load_options"]
        )
        scan_output = "\n".join(
            line for line in scan_output.splitlines() if "Created bg thread" not in line
        )
        self.assertEqual("key ==> value", scan_output.strip())
        dump = my_check_output(
            "./ldb manifest_dump --verbose --path={}".format(manifest_path),
            shell=True,
        )
        committed_options_numbers = [
            int(number)
            for number in re.findall(r"CommittedOptionsFileNumber: ([0-9]+)", dump)
        ]
        self.assertTrue(committed_options_numbers)
        self.assertEqual(rebound_options_number, committed_options_numbers[-1])

        # OPTIONS does not serialize DB/CF data roots. Operators provide those
        # roots explicitly so repair cannot reuse a file number already present
        # outside the DB directory.
        external_path = os.path.join(self.TMP_DIR, "external_data")
        os.mkdir(external_path)
        external_file_number = 123456
        open(
            os.path.join(external_path, "{:06d}.sst".format(external_file_number)),
            "a",
        ).close()
        self.assertRunOK(
            "update_manifest --rebind_options_file --options_file={} "
            "--file_number_paths={}".format(selected_options, external_path),
            "Manifest updates successful",
        )
        rebound_options_number = max(
            int(os.path.basename(path).split("-")[1])
            for path in glob.glob(os.path.join(db_path, "OPTIONS-*"))
        )
        self.assertGreater(rebound_options_number, external_file_number)
        self.assertRunError(
            "update_manifest --rebind_options_file --options_file={} "
            "--file_number_paths={}".format(
                selected_options, os.path.join(self.TMP_DIR, "missing_data_root")
            )
        )

        # A recoverable WAL can create L0/blob outputs before the normal open
        # allocations. The same boundary that is safe for a clean DB must be
        # rejected when it cannot accommodate those recovery outputs.
        recovery_headroom_db = os.path.join(
            self.TMP_DIR, "rebind_recovery_headroom_db"
        )
        self.assertRunOKFull(
            "put --db={} --create_if_missing key value".format(
                recovery_headroom_db
            ),
            "OK",
        )
        recovery_selected = os.path.basename(
            max(glob.glob(os.path.join(recovery_headroom_db, "OPTIONS-*")))
        )
        recovery_selected_path = os.path.join(
            recovery_headroom_db, recovery_selected
        )
        with open(recovery_selected_path, "r+", encoding="utf-8") as options_file:
            options_contents = options_file.read().replace(
                "track_options_file_number_in_manifest=false",
                "track_options_file_number_in_manifest=true",
            )
            options_file.seek(0)
            options_file.write(options_contents)
            options_file.truncate()
        recovery_boundary_options = "OPTIONS-{}".format(
            max_file_number - writable_open_headroom - 1
        )
        shutil.copyfile(
            os.path.join(recovery_headroom_db, recovery_selected),
            os.path.join(recovery_headroom_db, recovery_boundary_options),
        )
        self.assertRunErrorFull(
            "update_manifest --db={} --rebind_options_file --options_file={}".format(
                recovery_headroom_db, recovery_selected
            )
        )

        # The immediately lower clean boundary is repairable and must survive
        # writable-open allocations that depend on persisted DBOptions.
        headroom_db = os.path.join(self.TMP_DIR, "rebind_headroom_db")
        headroom_dump = os.path.join(self.TMP_DIR, "rebind_headroom_dump")
        self.assertTrue(self.dumpDb("--db={}".format(db_path), headroom_dump))
        self.assertTrue(
            self.loadDb(
                "--db={} --create_if_missing --disable_wal".format(headroom_db),
                headroom_dump,
            )
        )
        headroom_selected = os.path.basename(
            max(glob.glob(os.path.join(headroom_db, "OPTIONS-*")))
        )
        headroom_selected_path = os.path.join(headroom_db, headroom_selected)
        with open(headroom_selected_path, "r+", encoding="utf-8") as options_file:
            options_contents = options_file.read()
            self.assertIn("async_wal_precreate=false", options_contents)
            self.assertIn("persist_stats_to_disk=false", options_contents)
            self.assertIn("max_manifest_file_size=1073741824", options_contents)
            options_contents = options_contents.replace(
                "async_wal_precreate=false", "async_wal_precreate=true"
            ).replace(
                "persist_stats_to_disk=false", "persist_stats_to_disk=true"
            ).replace(
                "max_manifest_file_size=1073741824", "max_manifest_file_size=1"
            ).replace(
                "track_options_file_number_in_manifest=false",
                "track_options_file_number_in_manifest=true",
            )
            options_file.seek(0)
            options_file.write(options_contents)
            options_file.truncate()
        safe_boundary_options = "OPTIONS-{}".format(
            max_file_number - writable_open_headroom - 1
        )
        shutil.copyfile(
            headroom_selected_path,
            os.path.join(headroom_db, safe_boundary_options),
        )
        self.assertRunOKFull(
            "update_manifest --db={} --rebind_options_file --options_file={}".format(
                headroom_db, headroom_selected
            ),
            "Manifest updates successful",
        )
        self.assertRunOKFull(
            "put --db={} writable_open verified".format(headroom_db), "OK"
        )
        self.assertRunOKFull(
            "create_column_family --db={} new_cf".format(headroom_db), "OK"
        )

    def dumpDb(self, params, dumpFile):
        return 0 == run_err_null("./ldb dump {} > {}".format(params, dumpFile))

    def loadDb(self, params, dumpFile):
        return 0 == run_err_null("cat {} | ./ldb load {}".format(dumpFile, params))

    def writeExternSst(self, params, inputDumpFile, outputSst):
        return 0 == run_err_null(
            "cat {} | ./ldb write_extern_sst {} {}".format(
                inputDumpFile, outputSst, params
            )
        )

    def ingestExternSst(self, params, inputSst):
        return 0 == run_err_null(
            "./ldb ingest_extern_sst {} {}".format(inputSst, params)
        )

    def testStringBatchPut(self):
        print("Running testStringBatchPut...")
        self.assertRunOK("batchput x1 y1 --create_if_missing", "OK")
        self.assertRunOK("scan", "x1 ==> y1")
        self.assertRunOK('batchput x2 y2 x3 y3 "x4 abc" "y4 xyz"', "OK")
        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 abc ==> y4 xyz")
        self.assertRunFAIL("batchput")
        self.assertRunFAIL("batchput k1")
        self.assertRunFAIL("batchput k1 v1 k2")

    def testBlobBatchPut(self):
        print("Running testBlobBatchPut...")

        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("batchput x1 y1 --create_if_missing --enable_blob_files", "OK")
        self.assertRunOK("scan", "x1 ==> y1")
        self.assertRunOK(
            'batchput --enable_blob_files x2 y2 x3 y3 "x4 abc" "y4 xyz"', "OK"
        )
        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 abc ==> y4 xyz")

        blob_files = self.getBlobFiles(dbPath)
        self.assertTrue(len(blob_files) >= 1)

    def testBlobPut(self):
        print("Running testBlobPut...")

        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("put --create_if_missing --enable_blob_files x1 y1", "OK")
        self.assertRunOK("get x1", "y1")
        self.assertRunOK("put --enable_blob_files x2 y2", "OK")
        self.assertRunOK("get x1", "y1")
        self.assertRunOK("get x2", "y2")
        self.assertRunFAIL("get x3")

        blob_files = self.getBlobFiles(dbPath)
        self.assertTrue(len(blob_files) >= 1)

    def testBlobStartingLevel(self):
        print("Running testBlobStartingLevel...")

        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK(
            "put --create_if_missing --enable_blob_files --blob_file_starting_level=10 x1 y1",
            "OK",
        )
        self.assertRunOK("get x1", "y1")

        blob_files = self.getBlobFiles(dbPath)
        self.assertTrue(len(blob_files) == 0)

        self.assertRunOK(
            "put --enable_blob_files --blob_file_starting_level=0 x2 y2", "OK"
        )
        self.assertRunOK("get x1", "y1")
        self.assertRunOK("get x2", "y2")
        self.assertRunFAIL("get x3")

        blob_files = self.getBlobFiles(dbPath)
        self.assertTrue(len(blob_files) >= 1)

    def testCountDelimDump(self):
        print("Running testCountDelimDump...")
        self.assertRunOK("batchput x.1 x1 --create_if_missing", "OK")
        self.assertRunOK("batchput y.abc abc y.2 2 z.13c pqr", "OK")
        self.assertRunOK(
            "dump --count_delim",
            "x => count:1\tsize:5\ny => count:2\tsize:12\nz => count:1\tsize:8",
        )
        self.assertRunOK(
            'dump --count_delim="."',
            "x => count:1\tsize:5\ny => count:2\tsize:12\nz => count:1\tsize:8",
        )
        self.assertRunOK("batchput x,2 x2 x,abc xabc", "OK")
        self.assertRunOK(
            'dump --count_delim=","',
            "x => count:2\tsize:14\nx.1 => count:1\tsize:5\ny.2 => count:1\tsize:4\ny.abc => count:1\tsize:8\nz.13c => count:1\tsize:8",
        )

    def testCountDelimIDump(self):
        print("Running testCountDelimIDump...")
        self.assertRunOK("batchput x.1 x1 --create_if_missing", "OK")
        self.assertRunOK("batchput y.abc abc y.2 2 z.13c pqr", "OK")
        self.assertRunOK(
            "idump --count_delim",
            "x => count:1\tsize:5\ny => count:2\tsize:12\nz => count:1\tsize:8",
        )
        self.assertRunOK(
            'idump --count_delim="."',
            "x => count:1\tsize:5\ny => count:2\tsize:12\nz => count:1\tsize:8",
        )
        self.assertRunOK("batchput x,2 x2 x,abc xabc", "OK")
        self.assertRunOK(
            'idump --count_delim=","',
            "x => count:2\tsize:14\nx.1 => count:1\tsize:5\ny.2 => count:1\tsize:4\ny.abc => count:1\tsize:8\nz.13c => count:1\tsize:8",
        )

    def testInvalidCmdLines(self):
        print("Running testInvalidCmdLines...")
        # db not specified
        self.assertRunFAILFull("put 0x6133 0x6233 --hex --create_if_missing")
        # No param called he
        self.assertRunFAIL("put 0x6133 0x6233 --he --create_if_missing")
        # max_keys is not applicable for put
        self.assertRunFAIL("put 0x6133 0x6233 --max_keys=1 --create_if_missing")
        # hex has invalid boolean value

    def testHexPutGet(self):
        print("Running testHexPutGet...")
        self.assertRunOK("put a1 b1 --create_if_missing", "OK")
        self.assertRunOK("scan", "a1 ==> b1")
        self.assertRunOK("scan --hex", "0x6131 ==> 0x6231")
        self.assertRunFAIL("put --hex 6132 6232")
        self.assertRunOK("put --hex 0x6132 0x6232", "OK")
        self.assertRunOK("scan --hex", "0x6131 ==> 0x6231\n0x6132 ==> 0x6232")
        self.assertRunOK("scan", "a1 ==> b1\na2 ==> b2")
        self.assertRunOK("get a1", "b1")
        self.assertRunOK("get --hex 0x6131", "0x6231")
        self.assertRunOK("get a2", "b2")
        self.assertRunOK("get --hex 0x6132", "0x6232")
        self.assertRunOK(
            "multi_get --hex 0x6131 0x6132", "0x6131 ==> 0x6231\n0x6132 ==> 0x6232"
        )
        self.assertRunOK(
            "multi_get --hex 0x6131 0xBEEF", "0x6131 ==> 0x6231\nKey not found: 0xBEEF"
        )
        self.assertRunOK("get --key_hex 0x6132", "b2")
        self.assertRunOK("get --key_hex --value_hex 0x6132", "0x6232")
        self.assertRunOK("get --value_hex a2", "0x6232")
        self.assertRunOK(
            "scan --key_hex --value_hex", "0x6131 ==> 0x6231\n0x6132 ==> 0x6232"
        )
        self.assertRunOK(
            "scan --hex --from=0x6131 --to=0x6133",
            "0x6131 ==> 0x6231\n0x6132 ==> 0x6232",
        )
        self.assertRunOK("scan --hex --from=0x6131 --to=0x6132", "0x6131 ==> 0x6231")
        self.assertRunOK("scan --key_hex", "0x6131 ==> b1\n0x6132 ==> b2")
        self.assertRunOK("scan --value_hex", "a1 ==> 0x6231\na2 ==> 0x6232")
        self.assertRunOK("batchput --hex 0x6133 0x6233 0x6134 0x6234", "OK")
        self.assertRunOK("scan", "a1 ==> b1\na2 ==> b2\na3 ==> b3\na4 ==> b4")
        self.assertRunOK("delete --hex 0x6133", "OK")
        self.assertRunOK("scan", "a1 ==> b1\na2 ==> b2\na4 ==> b4")
        self.assertRunOK("checkconsistency", "OK")

    def testTtlPutGet(self):
        print("Running testTtlPutGet...")
        self.assertRunOK("put a1 b1 --ttl --create_if_missing", "OK")
        self.assertRunOK("scan --hex", "0x6131 ==> 0x6231", True)
        self.assertRunOK("dump --ttl ", "a1 ==> b1", True)
        self.assertRunOK("dump --hex --ttl ", "0x6131 ==> 0x6231\nKeys in range: 1")
        self.assertRunOK("scan --hex --ttl", "0x6131 ==> 0x6231")
        self.assertRunOK("get --value_hex a1", "0x6231", True)
        self.assertRunOK("get --ttl a1", "b1")
        self.assertRunOK("put a3 b3 --create_if_missing", "OK")
        # fails because timstamp's length is greater than value's
        self.assertRunFAIL("get --ttl a3")
        self.assertRunOK("checkconsistency", "OK")

    def testTxnPutGet(self):
        print("Running testTxnPutGet...")
        # Test basic put/get with TransactionDB (WriteCommitted - default)
        self.assertRunOK("put t1 v1 --use_txn --create_if_missing", "OK")
        self.assertRunOK("put t2 v2 --use_txn", "OK")
        self.assertRunOK("put t3 v3 --use_txn", "OK")
        # Verify data can be read back with TransactionDB
        self.assertRunOK("batchput t4 v4 t5 v5 --use_txn", "OK")

        # Test with WritePrepared policy (txn_write_policy=1)
        self.assertRunOK("put t6 v6 --use_txn --txn_write_policy=1", "OK")

        # Test with WriteUnprepared policy (txn_write_policy=2)
        self.assertRunOK("put t7 v7 --use_txn --txn_write_policy=2", "OK")

        # Verify all data persists and can be read without --use_txn
        # (regular DB::Open should work for WriteCommitted data)
        self.assertRunOK(
            "scan",
            "t1 ==> v1\nt2 ==> v2\nt3 ==> v3\nt4 ==> v4\nt5 ==> v5\nt6 ==> v6\nt7 ==> v7",
        )

        # Test delete with TransactionDB
        self.assertRunOK("delete t3 --use_txn", "OK")
        self.assertRunOK(
            "scan",
            "t1 ==> v1\nt2 ==> v2\nt4 ==> v4\nt5 ==> v5\nt6 ==> v6\nt7 ==> v7",
        )

        # Verify that --use_txn and --ttl cannot be used together
        self.assertRunFAIL("put x1 y1 --use_txn --ttl --create_if_missing")

        # Verify invalid txn_write_policy values are handled
        # (values outside 0-2 should fall back to 0)
        self.assertRunOK("put t8 v8 --use_txn --txn_write_policy=0", "OK")

        self.assertRunOK("checkconsistency", "OK")

    def testInvalidCmdLines(self):  # noqa: F811 T25377293 Grandfathered in
        print("Running testInvalidCmdLines...")
        # db not specified
        self.assertRunFAILFull("put 0x6133 0x6233 --hex --create_if_missing")
        # No param called he
        self.assertRunFAIL("put 0x6133 0x6233 --he --create_if_missing")
        # max_keys is not applicable for put
        self.assertRunFAIL("put 0x6133 0x6233 --max_keys=1 --create_if_missing")
        # hex has invalid boolean value
        self.assertRunFAIL("put 0x6133 0x6233 --hex=Boo --create_if_missing")

    def testDumpLoad(self):
        print("Running testDumpLoad...")
        self.assertRunOK("batchput --create_if_missing x1 y1 x2 y2 x3 y3 x4 y4", "OK")
        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4")
        origDbPath = os.path.join(self.TMP_DIR, self.DB_NAME)

        # Dump and load without any additional params specified
        dumpFilePath = os.path.join(self.TMP_DIR, "dump1")
        loadedDbPath = os.path.join(self.TMP_DIR, "loaded_from_dump1")
        self.assertTrue(self.dumpDb("--db=%s" % origDbPath, dumpFilePath))
        self.assertTrue(
            self.loadDb("--db=%s --create_if_missing" % loadedDbPath, dumpFilePath)
        )
        self.assertRunOKFull(
            "scan --db=%s" % loadedDbPath, "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4"
        )

        # Dump and load in hex
        dumpFilePath = os.path.join(self.TMP_DIR, "dump2")
        loadedDbPath = os.path.join(self.TMP_DIR, "loaded_from_dump2")
        self.assertTrue(self.dumpDb("--db=%s --hex" % origDbPath, dumpFilePath))
        self.assertTrue(
            self.loadDb(
                "--db=%s --hex --create_if_missing" % loadedDbPath, dumpFilePath
            )
        )
        self.assertRunOKFull(
            "scan --db=%s" % loadedDbPath, "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4"
        )

        # Dump only a portion of the key range
        dumpFilePath = os.path.join(self.TMP_DIR, "dump3")
        loadedDbPath = os.path.join(self.TMP_DIR, "loaded_from_dump3")
        self.assertTrue(
            self.dumpDb("--db=%s --from=x1 --to=x3" % origDbPath, dumpFilePath)
        )
        self.assertTrue(
            self.loadDb("--db=%s --create_if_missing" % loadedDbPath, dumpFilePath)
        )
        self.assertRunOKFull("scan --db=%s" % loadedDbPath, "x1 ==> y1\nx2 ==> y2")

        # Dump upto max_keys rows
        dumpFilePath = os.path.join(self.TMP_DIR, "dump4")
        loadedDbPath = os.path.join(self.TMP_DIR, "loaded_from_dump4")
        self.assertTrue(self.dumpDb("--db=%s --max_keys=3" % origDbPath, dumpFilePath))
        self.assertTrue(
            self.loadDb("--db=%s --create_if_missing" % loadedDbPath, dumpFilePath)
        )
        self.assertRunOKFull(
            "scan --db=%s" % loadedDbPath, "x1 ==> y1\nx2 ==> y2\nx3 ==> y3"
        )

        # Load into an existing db, create_if_missing is not specified
        self.assertTrue(self.dumpDb("--db=%s" % origDbPath, dumpFilePath))
        self.assertTrue(self.loadDb("--db=%s" % loadedDbPath, dumpFilePath))
        self.assertRunOKFull(
            "scan --db=%s" % loadedDbPath, "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4"
        )

        # Dump and load with WAL disabled
        dumpFilePath = os.path.join(self.TMP_DIR, "dump5")
        loadedDbPath = os.path.join(self.TMP_DIR, "loaded_from_dump5")
        self.assertTrue(self.dumpDb("--db=%s" % origDbPath, dumpFilePath))
        self.assertTrue(
            self.loadDb(
                "--db=%s --disable_wal --create_if_missing" % loadedDbPath, dumpFilePath
            )
        )
        self.assertRunOKFull(
            "scan --db=%s" % loadedDbPath, "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4"
        )

        # Dump and load with lots of extra params specified
        extraParams = " ".join(
            [
                "--bloom_bits=14",
                "--block_size=1024",
                "--auto_compaction=true",
                "--write_buffer_size=4194304",
                "--file_size=2097152",
            ]
        )
        dumpFilePath = os.path.join(self.TMP_DIR, "dump6")
        loadedDbPath = os.path.join(self.TMP_DIR, "loaded_from_dump6")
        self.assertTrue(
            self.dumpDb("--db={} {}".format(origDbPath, extraParams), dumpFilePath)
        )
        self.assertTrue(
            self.loadDb(
                "--db={} {} --create_if_missing".format(loadedDbPath, extraParams),
                dumpFilePath,
            )
        )
        self.assertRunOKFull(
            "scan --db=%s" % loadedDbPath, "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4"
        )

        # Dump with count_only
        dumpFilePath = os.path.join(self.TMP_DIR, "dump7")
        loadedDbPath = os.path.join(self.TMP_DIR, "loaded_from_dump7")
        self.assertTrue(self.dumpDb("--db=%s --count_only" % origDbPath, dumpFilePath))
        self.assertTrue(
            self.loadDb("--db=%s --create_if_missing" % loadedDbPath, dumpFilePath)
        )
        # DB should have atleast one value for scan to work
        self.assertRunOKFull("put --db=%s k1 v1" % loadedDbPath, "OK")
        self.assertRunOKFull("scan --db=%s" % loadedDbPath, "k1 ==> v1")

        # Dump command fails because of typo in params
        dumpFilePath = os.path.join(self.TMP_DIR, "dump8")
        self.assertFalse(
            self.dumpDb("--db=%s --create_if_missing" % origDbPath, dumpFilePath)
        )

        # Dump and load with BlobDB enabled
        blobParams = " ".join(
            ["--enable_blob_files", "--min_blob_size=1", "--blob_file_size=2097152"]
        )
        dumpFilePath = os.path.join(self.TMP_DIR, "dump9")
        loadedDbPath = os.path.join(self.TMP_DIR, "loaded_from_dump9")
        self.assertTrue(self.dumpDb("--db=%s" % (origDbPath), dumpFilePath))
        self.assertTrue(
            self.loadDb(
                "--db=%s %s --create_if_missing --disable_wal"
                % (loadedDbPath, blobParams),
                dumpFilePath,
            )
        )
        self.assertRunOKFull(
            "scan --db=%s" % loadedDbPath, "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4"
        )
        blob_files = self.getBlobFiles(loadedDbPath)
        self.assertTrue(len(blob_files) >= 1)

    def testIDumpBasics(self):
        print("Running testIDumpBasics...")
        self.assertRunOK("put a val --create_if_missing", "OK")
        self.assertRunOK("put b val", "OK")
        self.assertRunOK(
            "idump",
            "'a' seq:1, type:1 => val\n"
            "'b' seq:2, type:1 => val\nInternal keys in range: 2",
        )
        self.assertRunOK(
            "idump --input_key_hex --from={} --to={}".format(
                hex(ord("a")), hex(ord("b"))
            ),
            "'a' seq:1, type:1 => val\nInternal keys in range: 1",
        )

    def testIDumpDecodeBlobIndex(self):
        print("Running testIDumpDecodeBlobIndex...")
        self.assertRunOK("put a val --create_if_missing", "OK")
        self.assertRunOK("put b val --enable_blob_files", "OK")

        # Pattern to expect from dump with decode_blob_index flag enabled.
        regex = r".*\[blob ref\].*"
        expected_pattern = re.compile(regex)
        cmd = "idump %s --decode_blob_index"
        self.assertRunOKFull(
            (cmd) % (self.dbParam(self.DB_NAME)),
            expected_pattern,
            unexpected=False,
            isPattern=True,
        )

    def testMiscAdminTask(self):
        print("Running testMiscAdminTask...")
        # These tests need to be improved; for example with asserts about
        # whether compaction or level reduction actually took place.
        self.assertRunOK("batchput --create_if_missing x1 y1 x2 y2 x3 y3 x4 y4", "OK")
        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4")
        origDbPath = os.path.join(self.TMP_DIR, self.DB_NAME)

        self.assertTrue(0 == run_err_null("./ldb compact --db=%s" % origDbPath))
        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4")

        self.assertTrue(
            0 == run_err_null("./ldb reduce_levels --db=%s --new_levels=2" % origDbPath)
        )
        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4")

        self.assertTrue(
            0 == run_err_null("./ldb reduce_levels --db=%s --new_levels=3" % origDbPath)
        )
        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4")

        self.assertTrue(
            0 == run_err_null("./ldb compact --db=%s --from=x1 --to=x3" % origDbPath)
        )
        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4")

        self.assertTrue(
            0
            == run_err_null(
                "./ldb compact --db=%s --hex --from=0x6131 --to=0x6134" % origDbPath
            )
        )
        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4")

        # TODO(dilip): Not sure what should be passed to WAL.Currently corrupted.
        self.assertTrue(
            0
            == run_err_null(
                "./ldb dump_wal --db=%s --walfile=%s --header"
                % (origDbPath, origDbPath)
            )
        )
        self.assertRunOK("scan", "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4")

    def testCheckConsistency(self):
        print("Running testCheckConsistency...")

        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("put x1 y1 --create_if_missing", "OK")
        self.assertRunOK("put x2 y2", "OK")
        self.assertRunOK("get x1", "y1")
        self.assertRunOK("checkconsistency", "OK")

        sstFilePath = my_check_output(
            "ls %s" % os.path.join(dbPath, "*.sst"), shell=True
        )

        # Modify the file
        my_check_output("echo 'evil' > %s" % sstFilePath, shell=True)
        self.assertRunFAIL("checkconsistency")

        # Delete the file
        my_check_output("rm -f %s" % sstFilePath, shell=True)
        self.assertRunFAIL("checkconsistency")

    def dumpLiveFiles(self, params, dumpFile):
        return 0 == run_err_null(
            "./ldb dump_live_files {} > {}".format(params, dumpFile)
        )

    def testDumpLiveFiles(self):
        print("Running testDumpLiveFiles...")

        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("put x1 y1 --create_if_missing", "OK")
        self.assertRunOK("put x2 y2 --enable_blob_files", "OK")
        dumpFilePath = os.path.join(self.TMP_DIR, "dump1")
        self.assertTrue(self.dumpLiveFiles("--db=%s" % dbPath, dumpFilePath))
        self.assertRunOK("delete x1", "OK")
        self.assertRunOK("put x3 y3", "OK")
        dumpFilePath = os.path.join(self.TMP_DIR, "dump2")

        # Test that if the user provides a db path that ends with
        # a slash '/', there is no double (or more!) slashes in the
        # SST and manifest file names.

        # Add a '/' at the end of dbPath (which normally shouldnt contain any)
        if dbPath[-1] != "/":
            dbPath += "/"

        # Call the dump_live_files function with the edited dbPath name.
        self.assertTrue(
            self.dumpLiveFiles(
                "--db=%s --decode_blob_index --dump_uncompressed_blobs" % dbPath,
                dumpFilePath,
            )
        )

        # Investigate the output
        with open(dumpFilePath) as tmp:
            data = tmp.read()

        # Check that all the SST filenames have a correct full path (no multiple '/').
        sstFileList = re.findall(r"%s.*\d+.sst" % dbPath, data)
        self.assertTrue(len(sstFileList) >= 1)
        for sstFilename in sstFileList:
            filenumber = re.findall(r"\d+.sst", sstFilename)[0]
            self.assertEqual(sstFilename, dbPath + filenumber)

        # Check that all the Blob filenames have a correct full path (no multiple '/').
        blobFileList = re.findall(r"%s.*\d+.blob" % dbPath, data)
        self.assertTrue(len(blobFileList) >= 1)
        for blobFilename in blobFileList:
            filenumber = re.findall(r"\d+.blob", blobFilename)[0]
            self.assertEqual(blobFilename, dbPath + filenumber)

        # Check that all the manifest filenames
        # have a correct full path (no multiple '/').
        manifestFileList = re.findall(r"%s.*MANIFEST-\d+" % dbPath, data)
        self.assertTrue(len(manifestFileList) >= 1)
        for manifestFilename in manifestFileList:
            filenumber = re.findall(r"(?<=MANIFEST-)\d+", manifestFilename)[0]
            self.assertEqual(manifestFilename, dbPath + "MANIFEST-" + filenumber)

        # Check that the blob file index is decoded.
        decodedBlobIndex = re.findall(r"\[blob ref\]", data)
        self.assertTrue(len(decodedBlobIndex) >= 1)

    def listLiveFilesMetadata(self, params, dumpFile):
        return 0 == run_err_null(
            "./ldb list_live_files_metadata {} > {}".format(params, dumpFile)
        )

    def testListLiveFilesMetadata(self):
        print("Running testListLiveFilesMetadata...")

        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("put x1 y1 --create_if_missing", "OK")
        self.assertRunOK("put x2 y2", "OK")

        # Compare the SST filename and the level of list_live_files_metadata
        # with the data collected from dump_live_files.
        dumpFilePath1 = os.path.join(self.TMP_DIR, "dump1")
        self.assertTrue(self.dumpLiveFiles("--db=%s" % dbPath, dumpFilePath1))
        dumpFilePath2 = os.path.join(self.TMP_DIR, "dump2")
        self.assertTrue(
            self.listLiveFilesMetadata(
                "--sort_by_filename --db=%s" % dbPath, dumpFilePath2
            )
        )

        # Collect SST filename and level from dump_live_files
        with open(dumpFilePath1) as tmp:
            data = tmp.read()
            filename1 = re.findall(r".*\d+\.sst", data)[0]
            level1 = re.findall(r"level:\d+", data)[0].split(":")[1]

        # Collect SST filename and level from list_live_files_metadata
        with open(dumpFilePath2) as tmp:
            data = tmp.read()
            filename2 = re.findall(r".*\d+\.sst", data)[0]
            level2 = re.findall(r"level \d+", data)[0].split(" ")[1]

        # Assert equality between filenames and levels.
        self.assertEqual(filename1, filename2)
        self.assertEqual(level1, level2)

        # Create multiple column families and compare the output
        # of list_live_files_metadata with dump_live_files once again.
        # Create new CF, and insert data:
        self.assertRunOK("create_column_family mycol1", "OK")
        self.assertRunOK("put --column_family=mycol1 v1 v2", "OK")
        self.assertRunOK("create_column_family mycol2", "OK")
        self.assertRunOK("put --column_family=mycol2 h1 h2", "OK")
        self.assertRunOK("put --column_family=mycol2 h3 h4", "OK")

        # Call dump_live_files and list_live_files_metadata
        # and pipe the output to compare them later.
        dumpFilePath3 = os.path.join(self.TMP_DIR, "dump3")
        self.assertTrue(self.dumpLiveFiles("--db=%s" % dbPath, dumpFilePath3))
        dumpFilePath4 = os.path.join(self.TMP_DIR, "dump4")
        self.assertTrue(
            self.listLiveFilesMetadata(
                "--sort_by_filename --db=%s" % dbPath, dumpFilePath4
            )
        )

        # dump_live_files:
        # parse the output and create a map:
        # [key: sstFilename]->[value:[LSM level, Column Family Name]]
        referenceMap = {}
        with open(dumpFilePath3) as tmp:
            data = tmp.read()
            # Note: the following regex are contingent on what the
            # dump_live_files outputs.
            namesAndLevels = re.findall(r"\d+.sst level:\d+", data)
            cfs = re.findall(r"(?<=column family name=)\w+", data)
            # re.findall should not reorder the data.
            # Therefore namesAndLevels[i] matches the data from cfs[i].
            for count, nameAndLevel in enumerate(namesAndLevels):
                sstFilename = re.findall(r"\d+.sst", nameAndLevel)[0]
                sstLevel = re.findall(r"(?<=level:)\d+", nameAndLevel)[0]
                cf = cfs[count]
                referenceMap[sstFilename] = [sstLevel, cf]

        # list_live_files_metadata:
        # parse the output and create a map:
        # [key: sstFilename]->[value:[LSM level, Column Family Name]]
        testMap = {}
        with open(dumpFilePath4) as tmp:
            data = tmp.read()
            # Since for each SST file, all the information is contained
            # on one line, the parsing is easy to perform and relies on
            # the appearance of an "00xxx.sst" pattern.
            sstLines = re.findall(r".*\d+.sst.*", data)
            for line in sstLines:
                sstFilename = re.findall(r"\d+\.sst", line)[0]
                sstLevel = re.findall(r"(?<=level )\d+", line)[0]
                cf = re.findall(r"(?<=column family \')\w+(?=\')", line)[0]
                testMap[sstFilename] = [sstLevel, cf]

        # Compare the map obtained from dump_live_files and the map
        # obtained from list_live_files_metadata. Everything should match.
        self.assertEqual(referenceMap, testMap)

    def getManifests(self, directory):
        return glob.glob(directory + "/MANIFEST-*")

    def getSSTFiles(self, directory):
        return glob.glob(directory + "/*.sst")

    def getWALFiles(self, directory):
        return glob.glob(directory + "/*.log")

    def getBlobFiles(self, directory):
        return glob.glob(directory + "/*.blob")

    def copyManifests(self, src, dest):
        return 0 == run_err_null("cp " + src + " " + dest)

    def testManifestDump(self):
        print("Running testManifestDump...")
        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("put 1 1 --create_if_missing", "OK")
        self.assertRunOK("put 2 2", "OK")
        self.assertRunOK("put 3 3", "OK")
        # Pattern to expect from manifest_dump.
        num = "[0-9]+"
        st = ".*"
        subpat = st + " seq:" + num + ", type:" + num
        regex = num + ":" + num + r"\[" + subpat + ".." + subpat + r"\]"
        expected_pattern = re.compile(regex)
        cmd = "manifest_dump --db=%s"
        manifest_files = self.getManifests(dbPath)
        self.assertTrue(len(manifest_files) == 1)
        # Test with the default manifest file in dbPath.
        self.assertRunOKFull(
            cmd % dbPath, expected_pattern, unexpected=False, isPattern=True
        )
        self.copyManifests(manifest_files[0], manifest_files[0] + "1")
        manifest_files = self.getManifests(dbPath)
        self.assertTrue(len(manifest_files) == 2)
        # Test with multiple manifest files in dbPath.
        self.assertRunFAILFull(cmd % dbPath)
        # Running it with the copy we just created should pass.
        self.assertRunOKFull(
            (cmd + " --path=%s") % (dbPath, manifest_files[1]),
            expected_pattern,
            unexpected=False,
            isPattern=True,
        )
        # Make sure that using the dump with --path will result in identical
        # output as just using manifest_dump.
        cmd = "dump --path=%s"
        self.assertRunOKFull(
            (cmd) % (manifest_files[1]),
            expected_pattern,
            unexpected=False,
            isPattern=True,
        )

        # Check if null characters doesn't infer with output format.
        self.assertRunOK("put a1 b1", "OK")
        self.assertRunOK("put a2 b2", "OK")
        self.assertRunOK("put --hex 0x12000DA0 0x80C0000B", "OK")
        self.assertRunOK("put --hex 0x7200004f 0x80000004", "OK")
        self.assertRunOK("put --hex 0xa000000a 0xf000000f", "OK")
        self.assertRunOK("put a3 b3", "OK")
        self.assertRunOK("put a4 b4", "OK")

        # Verifies that all "levels" are printed out.
        # There should be 66 mentions of levels.
        expected_verbose_output = re.compile("matched")
        # Test manifest_dump verbose and verify that key 0x7200004f
        # is present. Note that we are forced to use grep here because
        # an output with a non-terminating null character in it isn't piped
        # correctly through the Python subprocess object.
        # Also note that 0x72=r and 0x4f=O, hence the regex \'r.{2}O\'
        # (we cannot use null character in the subprocess input either,
        # so we have to use '.{2}')
        cmd_verbose = (
            "manifest_dump --verbose --db=%s | grep -aq $''r.{2}O'' && echo 'matched' || echo 'not matched'"
            % dbPath
        )

        self.assertRunOKFull(
            cmd_verbose, expected_verbose_output, unexpected=False, isPattern=True
        )

    def testGetProperty(self):
        print("Running testGetProperty...")
        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("put 1 1 --create_if_missing", "OK")
        self.assertRunOK("put 2 2", "OK")
        # A "string" property
        cmd = "--db=%s get_property rocksdb.estimate-num-keys"
        self.assertRunOKFull(cmd % dbPath, "rocksdb.estimate-num-keys: 2")
        # A "map" property
        # FIXME: why doesn't this pick up two entries?
        cmd = "--db=%s get_property rocksdb.aggregated-table-properties"
        part = "rocksdb.aggregated-table-properties.num_entries: "
        expected_pattern = re.compile(part)
        self.assertRunOKFull(
            cmd % dbPath, expected_pattern, unexpected=False, isPattern=True
        )
        # An invalid property
        cmd = "--db=%s get_property rocksdb.this-property-does-not-exist"
        self.assertRunFAILFull(cmd % dbPath)

    def testSSTDump(self):
        print("Running testSSTDump...")

        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("put sst1 sst1_val --create_if_missing", "OK")
        self.assertRunOK("put sst2 sst2_val --enable_blob_files", "OK")
        self.assertRunOK("get sst1", "sst1_val")

        # Pattern to expect from SST dump.
        regex = ".*Sst file format:.*\n.*\\[blob ref\\].*"
        expected_pattern = re.compile(regex)

        sst_files = self.getSSTFiles(dbPath)
        self.assertTrue(len(sst_files) >= 1)
        cmd = "dump --path=%s --decode_blob_index"
        self.assertRunOKFull(
            (cmd) % (sst_files[0]), expected_pattern, unexpected=False, isPattern=True
        )

    def testBlobDump(self):
        print("Running testBlobDump")
        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("batchput x1 y1 --create_if_missing --enable_blob_files", "OK")
        self.assertRunOK(
            'batchput --enable_blob_files x2 y2 x3 y3 "x4 abc" "y4 xyz"', "OK"
        )

        # Pattern to expect from blob file dump.
        regex = r".*Blob log header[\s\S]*Blob log footer[\s\S]*Read record[\s\S]*Summary"  # noqa
        expected_pattern = re.compile(regex)
        blob_files = self.getBlobFiles(dbPath)
        self.assertTrue(len(blob_files) >= 1)
        cmd = "dump --path=%s --dump_uncompressed_blobs"
        self.assertRunOKFull(
            (cmd) % (blob_files[0]), expected_pattern, unexpected=False, isPattern=True
        )

    def testWALDump(self):
        print("Running testWALDump...")

        dbPath = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("put wal1 wal1_val --create_if_missing", "OK")
        self.assertRunOK("put wal2 wal2_val", "OK")
        self.assertRunOK("get wal1", "wal1_val")

        # Pattern to expect from WAL dump.
        regex = r"^Sequence,Count,ByteSize,Physical Offset,Key\(s\).*"
        expected_pattern = re.compile(regex)

        wal_files = self.getWALFiles(dbPath)
        self.assertTrue(len(wal_files) >= 1)
        cmd = "dump --path=%s"
        self.assertRunOKFull(
            (cmd) % (wal_files[0]), expected_pattern, unexpected=False, isPattern=True
        )

    def testListColumnFamilies(self):
        print("Running testListColumnFamilies...")
        self.assertRunOK("put x1 y1 --create_if_missing", "OK")
        cmd = 'list_column_families | grep -v "Column families"'
        # Test on valid dbPath.
        self.assertRunOK(cmd, "{default}")
        # Test on empty path.
        self.assertRunFAIL(cmd)

    def testColumnFamilies(self):
        print("Running testColumnFamilies...")
        _ = os.path.join(self.TMP_DIR, self.DB_NAME)
        self.assertRunOK("put cf1_1 1 --create_if_missing", "OK")
        self.assertRunOK("put cf1_2 2 --create_if_missing", "OK")
        self.assertRunOK("put cf1_3 3 --try_load_options", "OK")
        # Given non-default column family to single CF DB.
        self.assertRunFAIL("get cf1_1 --column_family=two")
        self.assertRunOK("create_column_family two", "OK")
        self.assertRunOK("put cf2_1 1 --create_if_missing --column_family=two", "OK")
        self.assertRunOK("put cf2_2 2 --create_if_missing --column_family=two", "OK")
        self.assertRunOK("delete cf1_2", "OK")
        self.assertRunOK("create_column_family three", "OK")
        self.assertRunOK("delete cf2_2 --column_family=two", "OK")
        self.assertRunOK("put cf3_1 3 --create_if_missing --column_family=three", "OK")
        self.assertRunOK("get cf1_1 --column_family=default", "1")
        self.assertRunOK("dump --column_family=two", "cf2_1 ==> 1\nKeys in range: 1")
        self.assertRunOK(
            "dump --column_family=two --try_load_options",
            "cf2_1 ==> 1\nKeys in range: 1",
        )
        self.assertRunOK("dump", "cf1_1 ==> 1\ncf1_3 ==> 3\nKeys in range: 2")
        self.assertRunOK("get cf2_1 --column_family=two", "1")
        self.assertRunOK("get cf3_1 --column_family=three", "3")
        self.assertRunOK("drop_column_family three", "OK")
        # non-existing column family.
        self.assertRunFAIL("get cf3_1 --column_family=four")
        self.assertRunFAIL("drop_column_family four")

    def testIngestExternalSst(self):
        print("Running testIngestExternalSst...")

        # Dump, load, write external sst and ingest it in another db
        dbPath = os.path.join(self.TMP_DIR, "db1")
        self.assertRunOK(
            "batchput --db=%s --create_if_missing x1 y1 x2 y2 x3 y3 x4 y4" % dbPath,
            "OK",
        )
        self.assertRunOK(
            "scan --db=%s" % dbPath, "x1 ==> y1\nx2 ==> y2\nx3 ==> y3\nx4 ==> y4"
        )
        dumpFilePath = os.path.join(self.TMP_DIR, "dump1")
        with open(dumpFilePath, "w") as f:
            f.write("x1 ==> y10\nx2 ==> y20\nx3 ==> y30\nx4 ==> y40")
        externSstPath = os.path.join(self.TMP_DIR, "extern_data1.sst")
        self.assertTrue(
            self.writeExternSst(
                "--create_if_missing --db=%s" % dbPath, dumpFilePath, externSstPath
            )
        )
        # cannot ingest if allow_global_seqno is false
        self.assertFalse(
            self.ingestExternSst(
                "--create_if_missing --allow_global_seqno=false --db=%s" % dbPath,
                externSstPath,
            )
        )
        self.assertTrue(
            self.ingestExternSst(
                "--create_if_missing --allow_global_seqno --db=%s" % dbPath,
                externSstPath,
            )
        )
        self.assertRunOKFull(
            "scan --db=%s" % dbPath, "x1 ==> y10\nx2 ==> y20\nx3 ==> y30\nx4 ==> y40"
        )


if __name__ == "__main__":
    unittest.main()
