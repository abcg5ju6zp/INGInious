# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" A plugin that allows to save submissions to a Git repository """
import queue
import io
import sys
import logging
import os.path
import shutil
import tarfile
import threading
import inginious.common.custom_yaml

if sys.platform == 'win32':
    import pbs
    git = pbs.Command('git')
else:
    from sh import git  # pylint: disable=no-name-in-module


class SubmissionGitSaver(threading.Thread):
    """
        Thread class that saves results from submission in the git repo.
        It must be a thread as a git commit can take some time and because we extract archives returned by the Client.
        But it must also be launched only one time as our git operations are not really process/tread-safe ;-)
    """

    def __init__(self, plugin_manager, config):
        threading.Thread.__init__(self)
        self._logger = logging.getLogger("inginious.webapp.plugins.SubmissionGitSaver")

        self.queue = queue.Queue()
        mustdoinit = False
        self.repopath = config.get("repo_directory", "./repo_submissions")
        if not os.path.exists(self.repopath):
            mustdoinit = True
            os.mkdir(self.repopath)
        self.git = git.bake('--work-tree=' + self.repopath, '--git-dir=' + os.path.join(self.repopath, '.git'))
        if mustdoinit:
            self.git.init()
        plugin_manager.add_hook('submission_done', self.add)
        plugin_manager.add_user_data_deletion_handler('git_repo', self.delete_user_data)
        self._logger.info("SubmissionGitSaver started")

    def _rewrite_identity(self, value, username, mode, pseudonym):
        """ Recursively erase/pseudonymize the username in parsed YAML content. """
        if isinstance(value, dict):
            return {k: self._rewrite_identity(v, username, mode, pseudonym) for k, v in value.items()}
        if isinstance(value, list):
            return [self._rewrite_identity(v, username, mode, pseudonym) for v in value]
        if isinstance(value, str):
            if value == username:
                return pseudonym if mode == "anonymize" else ""
            parts = value.split(",")
            if username in parts:
                parts = [self._rewrite_identity(p, username, mode, pseudonym) for p in parts if
                         not (mode == "erase" and p == username)]
                return ",".join(parts)
        return value

    def _rewrite_folder(self, path, username, mode, pseudonym):
        """ Strip identity from the yaml files of one saved submission folder. """
        for filename in ("input.yaml", "result.yaml"):
            filepath = os.path.join(path, filename)
            if not os.path.exists(filepath):
                continue
            try:
                with open(filepath, "r") as f:
                    content = inginious.common.custom_yaml.load(f)
                content = self._rewrite_identity(content, username, mode, pseudonym)
                with open(filepath, "w+") as f:
                    inginious.common.custom_yaml.dump(content, f)
            except Exception:
                self._logger.exception("Could not rewrite %s during user deletion", filepath)

    def delete_user_data(self, username=None, mode="erase", pseudonym=None, references=None, **kwargs):
        """ Called by the account deletion engine (plugin-declared user data). """
        references = references or []
        handled = set()
        for ref in references:
            if ref.get("type") != "submission":
                continue
            usernames = ref.get("usernames") or [username]
            if username not in usernames:
                continue
            task_dir = os.path.join(self.repopath, ref["courseid"], ref["taskid"])
            old_path = os.path.join(task_dir, "-".join(usernames))
            if old_path in handled or not os.path.exists(old_path):
                continue
            handled.add(old_path)

            remaining = [u for u in usernames if u != username]
            if mode == "erase" and not remaining:
                shutil.rmtree(old_path)
                continue

            self._rewrite_folder(old_path, username, mode, pseudonym)
            new_name = "-".join(remaining if mode == "erase" else remaining + [pseudonym])
            if new_name:
                new_path = os.path.join(task_dir, new_name)
                if os.path.exists(new_path):
                    shutil.rmtree(old_path)
                else:
                    os.rename(old_path, new_path)

        if handled:
            try:
                self.git.add('--all', '.')
                self.git.commit('-m', "Account deletion cleanup for " + str(username))
            except Exception:
                pass  # nothing to commit or git unavailable
        return None

    def add(self, submission, archive, _):
        """ Add a new submission to the repo (add the to queue, will be saved async)"""
        self.queue.put((submission, submission["result"], submission["grade"], submission["problems"], submission["tests"], submission["custom"], archive))

    def run(self):
        while True:
            try:
                submission, result, grade, problems, tests, custom, archive = self.queue.get()
                self.save(submission, result, grade, problems, tests, custom, archive)
            except Exception as inst:
                self._logger.exception("Exception in JobSaver: " + str(inst), exc_info=True)

    def save(self, submission, result, grade, problems, tests, custom, archive):  # pylint: disable=unused-argument
        """ saves a new submission in the repo (done async) """
        # Save submission to repo
        self._logger.info("Save submission " + str(submission["_id"]) + " to git repo")
        # Verify that the directory for the course exists
        if not os.path.exists(os.path.join(self.repopath, submission["courseid"])):
            os.mkdir(os.path.join(self.repopath, submission["courseid"]))
        # Idem with the task
        if not os.path.exists(os.path.join(self.repopath, submission["courseid"], submission["taskid"])):
            os.mkdir(os.path.join(self.repopath, submission["courseid"], submission["taskid"]))
        # Idem with the username, but empty it
        dirname = os.path.join(self.repopath, submission["courseid"], submission["taskid"], str.join("-", submission["username"]))
        if os.path.exists(dirname):
            shutil.rmtree(dirname)
        os.mkdir(dirname)
        # Now we can put the input, the output and the zip
        open(os.path.join(dirname, 'submitted_on'), "w+").write(str(submission["submitted_on"]))
        open(os.path.join(dirname, 'input.yaml'), "w+").write(inginious.common.custom_yaml.dump(submission["input"]))
        result_obj = {
            "result": result[0],
            "text": result[1],
            "problems": problems
        }
        open(os.path.join(dirname, 'result.yaml'), "w+").write(inginious.common.custom_yaml.dump(result_obj))
        if archive is not None:
            os.mkdir(os.path.join(dirname, 'output'))
            tar = tarfile.open(mode='r:gz', fileobj=io.BytesIO(archive))
            tar.extractall(os.path.join(dirname, 'output'))
            tar.close()

        self.git.add('--all', '.')
        title = " - ".join([str(submission["courseid"]) + "/" + str(submission["taskid"]),
                            str(submission["_id"]),
                            str.join("-", submission["username"]),
                            ("success" if result[0] == "success" else "failed")])
        self.git.commit('-m', title)


def init(plugin_manager, client, config):
    """
        Init the plugin

        Available configuration:
        ::

            plugins:
                - plugin_module: inginious.frontend.plugins.git_repo
                  repo_directory: "./repo_submissions"

    """
    submission_git_saver = SubmissionGitSaver(plugin_manager, config)
    submission_git_saver.daemon = True
    submission_git_saver.start()
