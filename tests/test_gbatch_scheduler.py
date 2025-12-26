import os
import json
import stat
import pytest
from panpath import PanPath
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch

from xqute.schedulers.gbatch_scheduler import GbatchScheduler
from xqute.defaults import JobStatus

from .conftest import BUCKET

MOCKS = Path(__file__).parent / "mocks"
# Make a fixture to get a unique workdir directory each time
WORKDIR = PanPath(f"{BUCKET}/xqute_gbatch_test")
WORKDIR = WORKDIR / str(uuid4())


def teardown_module():
    import asyncio
    asyncio.run(WORKDIR.a_rmtree())


@pytest.fixture(scope="module")
def gcloud():
    cmd = str(MOCKS / "gcloud")
    st = os.stat(cmd)
    os.chmod(cmd, st.st_mode | stat.S_IEXEC)
    return cmd


def test_error_with_non_gs_workdir(tmp_path):
    with pytest.raises(ValueError):
        GbatchScheduler(tmp_path, location="us-central1", project="test-project")


def test_error_with_invalid_jobname_prefix():
    with pytest.raises(ValueError):
        GbatchScheduler(
            project="test-project",
            location="us-central1",
            workdir=WORKDIR,
            jobname_prefix="1",
        )


def test_error_with_non_list_volumes_config():
    with pytest.raises(ValueError):
        GbatchScheduler(
            project="test-project",
            location="us-central1",
            workdir=WORKDIR,
            jobname_prefix="jobnameprefix",
            taskGroups=[{"taskSpec": {"volumes": 1}}],
        )


def test_cwd():
    """Test that the job script uses the correct working directory"""
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        workdir=WORKDIR,
        cwd="/custom/cwd",
    )
    assert "cd /custom/cwd" in scheduler.jobcmd_wrapper_init


async def test_labels():
    """Test that the job script includes labels"""
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        workdir=WORKDIR,
        labels={"key1": "value1", "key2": "value2"},
        allocationPolicy={"serviceAccount": {"email": "test-account"}},
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())
    assert conf["labels"]["key1"] == "value1"
    assert conf["labels"]["key2"] == "value2"
    assert conf["labels"]["xqute"] == "true"
    assert conf["labels"]["sacct"] == "test-account"


async def test_config_shortcuts():
    """Test that the job script includes config shortcuts"""
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        workdir=WORKDIR,
        mount="gs://my-bucket:/mnt/my-bucket",
        service_account="test-account@example.com",
        network="default-network",
        subnetwork="regions/us-central1/subnetworks/default",
        no_external_ip_address=True,
        machine_type="e2-standard-4",
        provisioning_model="SPOT",
        image_uri="ubuntu-2004-lts",
        entrypoint="/bin/bash",
        commands=["-c"],
        labels={"key1": "value1"},
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())
    assert conf["taskGroups"][0]["taskSpec"]["volumes"][-1] == {
        "gcs": {"remotePath": "my-bucket"},
        "mountPath": "/mnt/my-bucket",
    }

    assert conf["allocationPolicy"]["serviceAccount"]["email"] == (
        "test-account@example.com"
    )
    assert (
        conf["allocationPolicy"]["network"]["networkInterfaces"][0]["network"]
        == "default-network"
    )
    assert (
        conf["allocationPolicy"]["network"]["networkInterfaces"][0]["subnetwork"]
        == "regions/us-central1/subnetworks/default"
    )
    assert (
        conf["allocationPolicy"]["network"]["networkInterfaces"][0][
            "noExternalIpAddress"
        ]
        is True
    )
    assert conf["allocationPolicy"]["instances"][0]["policy"]["machineType"] == (
        "e2-standard-4"
    )
    assert (
        conf["allocationPolicy"]["instances"][0]["policy"]["provisioningModel"]
        == "SPOT"
    )
    assert (
        conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["image_uri"]
        == "ubuntu-2004-lts"
    )
    assert (
        conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["entrypoint"]
        == "/bin/bash"
    )
    assert conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"][
        "commands"
    ] == [
        "-c",
        "/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch",
    ]
    assert conf["labels"]["key1"] == "value1"
    assert conf["labels"]["xqute"] == "true"


async def test_shortcuts_not_overwrite_config():
    """Test that the job script includes config shortcuts"""
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        workdir=WORKDIR,
        mount="gs://my-bucket:/mnt/my-bucket",
        service_account="test-account@example.com",
        network="default-network",
        subnetwork="regions/us-central1/subnetworks/default",
        no_external_ip_address=True,
        machine_type="e2-standard-4",
        provisioning_model="SPOT",
        image_uri="ubuntu-2004-lts",
        entrypoint="/bin/bash",
        labels={"key1": "value1"},
        commands=["-d"],
        allocationPolicy={
            "serviceAccount": {"email": "other-account@example.com"},
            "network": {
                "networkInterfaces": [
                    {
                        "network": "other-network",
                        "subnetwork": "regions/us-central1/subnetworks/other",
                        "noExternalIpAddress": False,
                    }
                ]
            },
            "instances": [
                {
                    "policy": {
                        "machineType": "n1-standard-1",
                        "provisioningModel": "STANDARD",
                    }
                }
            ],
        },
        taskGroups=[
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "image_uri": "other-image",
                                "entrypoint": "/bin/bash2",
                                "commands": ["-c"],
                            }
                        }
                    ],
                    "volumes": [
                        {
                            "gcs": {"remotePath": "other-bucket"},
                            "mountPath": "/mnt/other-bucket",
                        }
                    ],
                }
            }
        ],
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())
    assert conf["taskGroups"][0]["taskSpec"]["volumes"][-2] == {
        "gcs": {"remotePath": "other-bucket"},
        "mountPath": "/mnt/other-bucket",
    }
    assert conf["taskGroups"][0]["taskSpec"]["volumes"][-1] == {
        "gcs": {"remotePath": "my-bucket"},
        "mountPath": "/mnt/my-bucket",
    }
    assert conf["allocationPolicy"]["serviceAccount"]["email"] == (
        "other-account@example.com"
    )
    assert (
        conf["allocationPolicy"]["network"]["networkInterfaces"][0]["network"]
        == "other-network"
    )
    assert (
        conf["allocationPolicy"]["network"]["networkInterfaces"][0]["subnetwork"]
        == "regions/us-central1/subnetworks/other"
    )
    assert (
        conf["allocationPolicy"]["network"]["networkInterfaces"][0][
            "noExternalIpAddress"
        ]
        is False
    )
    assert conf["allocationPolicy"]["instances"][0]["policy"]["machineType"] == (
        "n1-standard-1"
    )
    assert (
        conf["allocationPolicy"]["instances"][0]["policy"]["provisioningModel"]
        == "STANDARD"
    )
    assert (
        conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["image_uri"]
        == "other-image"
    )
    assert (
        conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]["entrypoint"]
        == "/bin/bash2"
    )
    assert conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"][
        "commands"
    ] == [
        "-c",
        "/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch",
    ]
    assert conf["labels"]["key1"] == "value1"
    assert conf["labels"]["xqute"] == "true"


async def test_named_mount_handling_in_gbatch():
    """Test handling of named mounts in GbatchScheduler"""

    # patch gbatch_scheduler.PanPath to avoid actual GCS access
    # PanPath.is_file should return True for bucket_file and False for bucket_dir
    # PanPath.parent should return the parent path correctly, which is also an PanPath
    # PanPath._no_prefix should return the path without the "gs://" prefix
    # PanPath.name should return the name of the path
    # PanPath.__str__ should return the full path
    # Create a simple mock class that behaves like PanPath
    class MockAnyPath:
        def __init__(self, path_str):
            self.path_str = str(path_str)
            self._path_obj = Path(self.path_str.replace("gs://", ""))

        def __str__(self):
            return self.path_str

        def is_file(self):
            return self.path_str.endswith("file.txt")

        @property
        def parent(self):
            if self.path_str.startswith("gs://"):
                parent_path = "gs://" + str(self._path_obj.parent)
            else:
                parent_path = str(self._path_obj.parent)
            return MockAnyPath(parent_path)

        @property
        def _no_prefix(self):
            if self.path_str.startswith("gs://"):
                return self.path_str[5:]
            return self.path_str

        @property
        def name(self):
            return self._path_obj.name

    with patch("xqute.schedulers.gbatch_scheduler.PanPath", MockAnyPath):
        bucket = "gs://my-bucket"
        bucket_dir = f"{bucket}/dir1"
        bucket_file = f"{bucket}/dir2/file.txt"

        scheduler = GbatchScheduler(
            project="test-project",
            location="us-central1",
            workdir=WORKDIR,
            mount=[f"DIR={bucket_dir}", f"FILE={bucket_file}"],
        )
        volumes = scheduler.config["taskGroups"][0]["taskSpec"]["volumes"]
        assert len(volumes) == 3
        assert volumes[1] == {
            "gcs": {"remotePath": "my-bucket/dir1"},
            "mountPath": "/mnt/disks/DIR",
        }
        assert volumes[2] == {
            "gcs": {"remotePath": "my-bucket/dir2"},
            "mountPath": "/mnt/disks/FILE/dir2",
        }
        assert scheduler._path_envs["DIR"] == "/mnt/disks/DIR"
        assert scheduler._path_envs["FILE"] == "/mnt/disks/FILE/dir2/file.txt"

        job = await scheduler.create_job(0, ["echo", 1])
        init_cmd = scheduler.jobcmd_init(job)
        assert 'export DIR=/mnt/disks/DIR' in init_cmd
        assert 'export FILE=/mnt/disks/FILE/dir2/file.txt' in init_cmd


def test_named_mount_is_not_gs_path():
    """Test error is raised if named mount is not a gs:// path"""
    expected_msg = "When using named mount"
    with pytest.raises(ValueError, match=expected_msg):
        GbatchScheduler(
            project="test-project",
            location="us-central1",
            workdir=WORKDIR,
            mount=["DIR=/local/path"],
        )


async def test_job():
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        submission_batch=1,
    )
    assert scheduler.subm_batch == 1
    job = await scheduler.create_job(0, ["echo", 1])
    assert (
        await scheduler.wrapped_job_script(job, _mounted=True)
        == scheduler.workdir / "0" / "job.wrapped.gbatch"
    )
    assert job.metadir.mounted == Path("/mnt/disks/xqute_workdir/0")

    script = scheduler.wrap_job_script(job)
    assert "/mnt/disks/xqute_workdir/0/job.status" in script


async def test_sched_with_container():
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        taskGroups=[
            {"taskSpec": {"runnables": [{"container": {"image_uri": "ubuntu"}}]}}
        ],
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    assert conf_file.name == "job.wrapped.gbatch.json"
    conf = json.loads(await conf_file.a_read_text())
    container = conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]
    assert container["image_uri"] == "ubuntu"
    assert container["commands"] == ["/mnt/disks/xqute_workdir/0/job.wrapped.gbatch"]
    assert container["entrypoint"] == "/bin/bash"


async def test_sched_with_container_entrypoint():
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        taskGroups=[
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "image_uri": "ubuntu",
                                "entrypoint": "/bin/bash2",
                                "commands": ["-c"],
                            }
                        }
                    ]
                }
            }
        ],
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    assert conf_file.name == "job.wrapped.gbatch.json"
    conf = json.loads(await conf_file.a_read_text())
    container = conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]
    assert container["image_uri"] == "ubuntu"
    assert container["commands"] == [
        "-c",
        "/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch",
    ]
    assert container["entrypoint"] == "/bin/bash2"


async def test_sched_with_container_command_template():
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        taskGroups=[
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "image_uri": "ubuntu",
                                "entrypoint": "/bin/bash2",
                                "commands": ["-c", "{lang}3 {script}"],
                            }
                        }
                    ]
                }
            }
        ],
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    assert conf_file.name == "job.wrapped.gbatch.json"
    conf = json.loads(await conf_file.a_read_text())
    container = conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]
    assert container["image_uri"] == "ubuntu"
    assert container["commands"] == [
        "-c",
        "/bin/bash3 /mnt/disks/xqute_workdir/0/job.wrapped.gbatch",
    ]
    assert container["entrypoint"] == "/bin/bash2"


async def test_sched_with_script_runnable():
    """Test scheduler with script runnable (no container)"""
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())

    script_runnable = conf["taskGroups"][0]["taskSpec"]["runnables"][0]["script"]
    expected_text = "/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch"
    assert script_runnable["text"] == expected_text
    # Ensure _commands is removed after text composition
    assert "_commands" not in script_runnable


async def test_sched_with_script_commands():
    """Test scheduler with script runnable and custom commands"""
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        commands=["-c", "echo starting"],
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())

    script_runnable = conf["taskGroups"][0]["taskSpec"]["runnables"][0]["script"]
    expected_text = (
        "-c 'echo starting' "
        "'/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch'"
    )
    assert script_runnable["text"] == expected_text
    assert "_commands" not in script_runnable


async def test_sched_with_script_commands_template():
    """Test scheduler with script runnable using command templates"""
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        commands=["{lang}", "-u {script}"],
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())

    script_runnable = conf["taskGroups"][0]["taskSpec"]["runnables"][0]["script"]
    expected_text = "'/bin/bash' '-u /mnt/disks/xqute_workdir/0/job.wrapped.gbatch'"
    assert script_runnable["text"] == expected_text
    assert "_commands" not in script_runnable


async def test_additional_runnables_basic():
    """Test basic additional runnables functionality"""
    runnables = [
        {"script": {"text": "echo setup"}},
        {"script": {"text": "echo cleanup"}},
    ]

    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        runnables=runnables,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())

    task_runnables = conf["taskGroups"][0]["taskSpec"]["runnables"]
    assert len(task_runnables) == 3  # 2 additional + 1 main job

    # Check that the main job runnable is at the expected index
    assert scheduler.runnable_index == 0
    expected_script = "/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch"
    assert task_runnables[scheduler.runnable_index]["script"]["text"] == expected_script


async def test_additional_runnables_with_ordering():
    """Test additional runnables with ordering"""
    runnables = [
        {"script": {"text": "echo after"}, "order": 1},
        {"script": {"text": "echo before"}, "order": -1},
        {"script": {"text": "echo middle"}, "order": 0},
    ]

    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        runnables=runnables,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())

    task_runnables = conf["taskGroups"][0]["taskSpec"]["runnables"]
    assert len(task_runnables) == 4  # 3 additional + 1 main job

    # Check ordering: before (-1), main job, middle (0), after (1)
    assert task_runnables[0]["script"]["text"] == "echo before"
    expected_script = "/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch"
    assert task_runnables[1]["script"]["text"] == expected_script
    assert task_runnables[2]["script"]["text"] == "echo middle"
    assert task_runnables[3]["script"]["text"] == "echo after"

    # Main job should be at index 1
    assert scheduler.runnable_index == 1


async def test_additional_runnables_with_container():
    """Test additional runnables work with container-based main job"""
    runnables = [
        {"script": {"text": "echo setup"}, "order": -1},
        {"script": {"text": "echo cleanup"}, "order": 1},
    ]

    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        image_uri="ubuntu",
        entrypoint="/bin/bash",
        commands=["-c"],
        runnables=runnables,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())

    task_runnables = conf["taskGroups"][0]["taskSpec"]["runnables"]
    assert len(task_runnables) == 3

    # Check setup runnable
    assert task_runnables[0]["script"]["text"] == "echo setup"

    # Check main container job
    container = task_runnables[1]["container"]
    assert container["image_uri"] == "ubuntu"
    assert container["entrypoint"] == "/bin/bash"
    expected_commands = [
        "-c",
        "/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch",
    ]
    assert container["commands"] == expected_commands

    # Check cleanup runnable
    assert task_runnables[2]["script"]["text"] == "echo cleanup"


async def test_additional_runnables_complex_ordering():
    """Test complex ordering scenarios"""
    runnables = [
        {"script": {"text": "echo step3"}, "order": 3},
        {"script": {"text": "echo step-2"}, "order": -2},
        {"script": {"text": "echo step1"}, "order": 1},
        {"script": {"text": "echo step-1"}, "order": -1},
        {"script": {"text": "echo step2"}, "order": 2},
    ]

    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        runnables=runnables,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())

    task_runnables = conf["taskGroups"][0]["taskSpec"]["runnables"]
    assert len(task_runnables) == 6  # 5 additional + 1 main job

    # Check ordering: -2, -1, main job, 1, 2, 3
    assert task_runnables[0]["script"]["text"] == "echo step-2"
    assert task_runnables[1]["script"]["text"] == "echo step-1"
    expected_script = "/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch"
    assert task_runnables[2]["script"]["text"] == expected_script
    assert task_runnables[3]["script"]["text"] == "echo step1"
    assert task_runnables[4]["script"]["text"] == "echo step2"
    assert task_runnables[5]["script"]["text"] == "echo step3"

    # Main job should be at index 2
    assert scheduler.runnable_index == 2


async def test_additional_runnables_no_order_key():
    """Test additional runnables without order key (defaults to 0)"""
    runnables = [
        {"script": {"text": "echo no-order-1"}},
        {"script": {"text": "echo no-order-2"}},
        {"script": {"text": "echo before"}, "order": -1},
    ]

    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        runnables=runnables,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())

    task_runnables = conf["taskGroups"][0]["taskSpec"]["runnables"]
    assert len(task_runnables) == 4

    # Check ordering: before (-1), main job, no-order-1 (0), no-order-2 (0)
    assert task_runnables[0]["script"]["text"] == "echo before"
    expected_script = "/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch"
    assert task_runnables[1]["script"]["text"] == expected_script
    assert task_runnables[2]["script"]["text"] == "echo no-order-1"
    assert task_runnables[3]["script"]["text"] == "echo no-order-2"


async def test_existing_runnable_with_additional():
    """Test that existing runnable configuration is preserved with
    additional runnables"""
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        taskGroups=[
            {
                "taskSpec": {
                    "runnables": [
                        {"container": {"image_uri": "existing-image", "commands": []}}
                    ]
                }
            }
        ],
        runnables=[
            {"script": {"text": "echo setup"}, "order": -1},
            {"script": {"text": "echo cleanup"}, "order": 1},
        ],
    )
    job = await scheduler.create_job(0, ["echo", 1])
    conf_file = await scheduler.job_config_file(job)
    conf = json.loads(await conf_file.a_read_text())

    task_runnables = conf["taskGroups"][0]["taskSpec"]["runnables"]
    assert len(task_runnables) == 3

    # Check that existing container configuration is preserved
    container = task_runnables[1]["container"]
    assert container["image_uri"] == "existing-image"
    assert container["entrypoint"] == "/bin/bash"
    assert container["commands"] == ["/mnt/disks/xqute_workdir/0/job.wrapped.gbatch"]


async def test_scheduler(gcloud):

    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        gcloud=gcloud,
        workdir=WORKDIR,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    assert (await scheduler.submit_job(job)).startswith("gbatch-")
    await job.set_jid("gbatch-36760976-0")
    await scheduler.kill_job(job)
    if await job.jid_file.a_is_file():
        await job.jid_file.a_unlink()
    job._jid = None
    assert not await job.jid_file.a_is_file()
    assert await scheduler.job_is_running(job) is False

    await job.jid_file.a_write_text("gbatch-36760976-0")
    assert await scheduler.job_is_running(job) is True

    await job.jid_file.a_write_text("wrongjobid")
    assert await scheduler.job_is_running(job) is False
    await job.jid_file.a_unlink()


async def test_submission_failure():
    gcloud = str(MOCKS / "no_such_gcloud")

    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        gcloud=gcloud,
        workdir=WORKDIR,
    )
    job = await scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert await job.status == JobStatus.FAILED
    assert "Failed to submit job" in await job.stderr_file.a_read_text()
