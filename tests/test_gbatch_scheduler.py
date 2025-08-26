import os
import json
import stat
import pytest
from cloudpathlib import AnyPath
from pathlib import Path
from uuid import uuid4

from xqute.schedulers.gbatch_scheduler import GbatchScheduler
from xqute.defaults import JobStatus

from .conftest import BUCKET

MOCKS = Path(__file__).parent / "mocks"
# Make a fixture to get a unique workdir directory each time
WORKDIR = AnyPath(f"{BUCKET}/xqute_gbatch_test")
WORKDIR = WORKDIR / str(uuid4())


def teardown_module():
    WORKDIR.rmtree()


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


def test_labels():
    """Test that the job script includes labels"""
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        workdir=WORKDIR,
        labels={"key1": "value1", "key2": "value2"},
        allocationPolicy={"serviceAccount": {"email": "test-account"}},
    )
    job = scheduler.create_job(0, ["echo", 1])
    conf_file = scheduler.job_config_file(job)
    conf = json.loads(conf_file.read_text())
    assert conf["labels"]["key1"] == "value1"
    assert conf["labels"]["key2"] == "value2"
    assert conf["labels"]["xqute"] == "true"
    assert conf["labels"]["sacct"] == "test-account"


def test_config_shortcuts():
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
    job = scheduler.create_job(0, ["echo", 1])
    conf_file = scheduler.job_config_file(job)
    conf = json.loads(conf_file.read_text())
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


def test_shortcuts_not_overwrite_config():
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
    job = scheduler.create_job(0, ["echo", 1])
    conf_file = scheduler.job_config_file(job)
    conf = json.loads(conf_file.read_text())
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


def test_job():
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
    )
    job = scheduler.create_job(0, ["echo", 1])
    assert (
        scheduler.wrapped_job_script(job)
        == scheduler.workdir / "0" / "job.wrapped.gbatch"
    )
    assert job.metadir.mounted == Path("/mnt/disks/xqute_workdir/0")

    script = scheduler.wrap_job_script(job)
    assert "/mnt/disks/xqute_workdir/0/job.status" in script


def test_sched_with_container():
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
        taskGroups=[
            {"taskSpec": {"runnables": [{"container": {"image_uri": "ubuntu"}}]}}
        ],
    )
    job = scheduler.create_job(0, ["echo", 1])
    conf_file = scheduler.job_config_file(job)
    assert conf_file.name == "job.wrapped.gbatch.json"
    conf = json.loads(conf_file.read_text())
    container = conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]
    assert container["image_uri"] == "ubuntu"
    assert container["commands"] == ["/mnt/disks/xqute_workdir/0/job.wrapped.gbatch"]
    assert container["entrypoint"] == "/bin/bash"


def test_sched_with_container_entrypoint():
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
    job = scheduler.create_job(0, ["echo", 1])
    conf_file = scheduler.job_config_file(job)
    assert conf_file.name == "job.wrapped.gbatch.json"
    conf = json.loads(conf_file.read_text())
    container = conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]
    assert container["image_uri"] == "ubuntu"
    assert container["commands"] == [
        "-c",
        "/bin/bash /mnt/disks/xqute_workdir/0/job.wrapped.gbatch",
    ]
    assert container["entrypoint"] == "/bin/bash2"


def test_sched_with_container_command_template():
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
    job = scheduler.create_job(0, ["echo", 1])
    conf_file = scheduler.job_config_file(job)
    assert conf_file.name == "job.wrapped.gbatch.json"
    conf = json.loads(conf_file.read_text())
    container = conf["taskGroups"][0]["taskSpec"]["runnables"][0]["container"]
    assert container["image_uri"] == "ubuntu"
    assert container["commands"] == [
        "-c",
        "/bin/bash3 /mnt/disks/xqute_workdir/0/job.wrapped.gbatch",
    ]
    assert container["entrypoint"] == "/bin/bash2"


@pytest.mark.asyncio
async def test_scheduler(gcloud):

    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        gcloud=gcloud,
        workdir=WORKDIR,
    )
    job = scheduler.create_job(0, ["echo", 1])
    assert (await scheduler.submit_job(job)).startswith("gbatch-")
    job.jid = "gbatch-36760976-0"
    await scheduler.kill_job(job)
    if job.jid_file.is_file():
        job.jid_file.unlink()
    job._jid = None
    assert not job.jid_file.is_file()
    assert await scheduler.job_is_running(job) is False

    job.jid_file.write_text("gbatch-36760976-0")
    assert await scheduler.job_is_running(job) is True

    job.jid_file.write_text("wrongjobid")
    assert await scheduler.job_is_running(job) is False
    job.jid_file.unlink()


@pytest.mark.asyncio
async def test_submission_failure():
    gcloud = str(MOCKS / "no_such_gcloud")

    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        gcloud=gcloud,
        workdir=WORKDIR,
    )
    job = scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()
