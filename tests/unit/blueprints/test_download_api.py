import pytest
import requests


@pytest.mark.usefixtures()
class TestGetRequest:
    endpoint = "/requests"

    def test_not_found(self, client, mock_metax, not_found_task):
        query_string = {"dataset": not_found_task["dataset_id"]}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 404

    def test_pending(self, client, mock_metax, pending_task):
        query_string = {"dataset": pending_task["dataset_id"]}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200

    def test_pending_partial(
        self, client, mock_metax, pending_task, pending_partial_task
    ):
        query_string = {"dataset": pending_task["dataset_id"]}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200

    def test_started(self, client, mock_metax, started_task):
        query_string = {"dataset": started_task["dataset_id"]}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200

    def test_success(self, client, mock_metax, success_task):
        query_string = {"dataset": success_task["dataset_id"]}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200

    def test_success_no_package(self, client, mock_metax, success_no_package_task):
        query_string = {"dataset": success_no_package_task["dataset_id"]}
        response = client.get(self.endpoint, query_string=query_string)
        json_response = response.get_json()

        assert response.status_code == 404

    def test_success_not_outdated(self, client, mock_metax, success_not_outdated_task):
        query_string = {"dataset": success_not_outdated_task["dataset_id"]}
        response = client.get(self.endpoint, query_string=query_string)
        json_response = response.get_json()

        assert response.status_code == 200

    def test_success_partial(
        self,
        client,
        mock_metax,
        success_task,
        success_partial_task,
        success_partial_2_task,
    ):
        query_string = {"dataset": success_partial_task["dataset_id"]}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200

    def test_dataset_has_to_be_specified_in_query(
        self, client, mock_metax, not_found_task
    ):
        query_string = {}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 400

    def test_dataset_that_cannot_be_found_in_metax(
        self, client, metax_dataset_not_found
    ):
        query_string = {"dataset": "1"}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 404
        assert "was not found in Metax API" in str(response.data)

    def test_cannot_connect_to_metax(self, client, metax_cannot_connect):
        query_string = {"dataset": "1"}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_missing_fields_in_metax_response(self, client, metax_missing_fields):
        query_string = {"dataset": "1"}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_unexpected_status_code_in_metax_response(
        self, client, metax_unexpected_status_code
    ):
        query_string = {"dataset": "1"}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)


@pytest.mark.usefixtures("mock_celery")
class TestPostRequest:
    endpoint = "/requests"

    def test_not_found(self, client, mock_metax, recorder, not_found_task):
        json = {"dataset": not_found_task["dataset_id"]}
        response = client.post(self.endpoint, json=json)
        json_response = response.get_json()

        assert response.status_code == 200
        assert json_response["created"] is True
        assert recorder.called is True

    def test_not_found_partial(self, client, mock_metax, recorder, not_found_task):
        json = {"dataset": not_found_task["dataset_id"], "scope": ["/test1/file1.txt"]}
        response = client.post(self.endpoint, json=json)
        json_response = response.get_json()

        assert response.status_code == 200
        assert json_response["created"] is True
        assert recorder.called is True

    def test_pending(
        self, client, mock_metax, recorder, pending_task, pending_partial_task
    ):
        json = {
            "dataset": pending_partial_task["dataset_id"],
            "scope": ["/test1/file1.txt"],
        }
        response = client.post(self.endpoint, json=json)
        json_response = response.get_json()

        assert response.status_code == 200
        assert json_response["created"] is False
        assert recorder.called

    def test_generation_request_with_identical_content(
        self, client, mock_metax, recorder, pending_partial_task
    ):
        response = client.post(
            self.endpoint,
            json={"dataset": pending_partial_task["dataset_id"], "scope": ["/test1"]},
        )
        json_response = response.get_json()

        assert response.status_code == 200
        assert json_response["created"] is False
        assert recorder.called

    def test_request_for_existing_succesful_task(
        self, client, mock_metax, success_task
    ):
        response = client.post(
            self.endpoint, json={"dataset": success_task["dataset_id"]}
        )
        assert response.status_code == 200
        assert response.get_json()["created"] is False

    def test_succesful_task_with_no_package(
        self, client, mock_metax, success_no_package_task
    ):
        response = client.post(
            self.endpoint, json={"dataset": success_no_package_task["dataset_id"]}
        )
        assert response.status_code == 200
        assert response.get_json()["created"] is True

    def test_request_for_existing_succesful_partial_task(
        self, client, mock_metax, success_partial_task
    ):
        response = client.post(
            self.endpoint,
            json={
                "dataset": success_partial_task["dataset_id"],
                "scope": ["/test2/file2.txt"],
            },
        )
        assert response.status_code == 200
        assert response.get_json()["created"] is False

    def test_dataset_has_to_be_specified_in_request(self, client, mock_metax):
        response = client.post(self.endpoint, json={})
        assert response.status_code == 400

    def test_dataset_that_cannot_be_found_in_metax(
        self, client, metax_dataset_not_found
    ):
        json = {"dataset": "1"}
        response = client.post(self.endpoint, json=json)
        assert response.status_code == 404
        assert "was not found in Metax API" in str(response.data)

    def test_cannot_connect_to_metax(self, client, metax_cannot_connect):
        json = {"dataset": "1"}
        response = client.post(self.endpoint, json=json)
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_cannot_connect_to_metax_for_getting_files(
        self, client, mock_metax, get_matching_dataset_files_connection_error
    ):
        json = {"dataset": "1"}
        response = client.post(self.endpoint, json=json)
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_unexpected_status_code_from_metax_for_getting_files(
        self, client, mock_metax, get_matching_dataset_files_unexpected_status_code
    ):
        json = {"dataset": "1"}
        response = client.post(self.endpoint, json=json)
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_missing_fields_in_metax_response(self, client, metax_missing_fields):
        json = {"dataset": "1"}
        response = client.post(self.endpoint, json=json)
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_unexpected_status_code_in_metax_response(
        self, client, metax_unexpected_status_code
    ):
        json = {"dataset": "1"}
        response = client.post(self.endpoint, json=json)
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_non_matching_scope(self, client, mock_metax):
        json = {"dataset": "1", "scope": ["/non"]}
        response = client.post(self.endpoint, json=json)
        assert response.status_code == 409
        assert "No matching files" in str(response.data)


class TestPostSubscribe:
    endpoint = "/subscribe"

    def test_valid_pending_task(self, client, mock_metax, pending_task):
        response = client.post(
            self.endpoint,
            json={
                "dataset": pending_task["dataset_id"],
                "subscriptionData": "aslrnlbrinrdlr",
                "notifyURL": "https://example.com/notify",
            },
        )
        assert response.status_code == 201

    def test_valid_pending_partial_task(self, client, mock_metax, pending_partial_task):
        response = client.post(
            self.endpoint,
            json={
                "dataset": pending_partial_task["dataset_id"],
                "scope": ["/test1/file1.txt"],
                "subscriptionData": "aslrnlbrinrdlr",
                "notifyURL": "https://example.com/notify",
            },
        )
        assert response.status_code == 201

    def test_valid_request_no_subscription_data(self, client, mock_metax, pending_task):
        response = client.post(
            self.endpoint,
            json={
                "dataset": pending_task["dataset_id"],
                "subscriptionData": "aslrnlbrinrdlr",
                "notifyURL": "https://example.com/notify",
            },
        )
        assert response.status_code == 201

    def test_invalid_request_missing_notify_url(self, client, mock_metax, pending_task):
        response = client.post(
            self.endpoint,
            json={
                "dataset": pending_task["dataset_id"],
                "subscriptionData": "aslrnlbrinrdlr",
            },
        )
        assert response.status_code == 400

    def test_not_found_task(self, client, mock_metax, not_found_task):
        response = client.post(
            self.endpoint,
            json={
                "dataset": not_found_task["dataset_id"],
                "subscriptionData": "aslrnlbrinrdlr",
                "notifyURL": "https://example.com/notify",
            },
        )
        assert response.status_code == 404

    def test_successful_task(self, client, mock_metax, success_task):
        response = client.post(
            self.endpoint,
            json={
                "dataset": success_task["dataset_id"],
                "subscriptionData": "aslrnlbrinrdlr",
                "notifyURL": "https://example.com/notify",
            },
        )
        assert response.status_code == 409


@pytest.mark.usefixtures()
class TestPostAuthorize:
    endpoint = "/authorize"

    def test_authorize_generated_package_download(
        self, client, recorder, mock_metax, success_task
    ):
        response = client.post(
            self.endpoint,
            json={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert recorder.called
        assert response.status_code == 200

    def test_authorize_outdated_generated_package_download(
        self, client, recorder, mock_metax_modified, success_task
    ):
        response = client.post(
            self.endpoint,
            json={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert recorder.called
        assert response.status_code == 409

    def test_authorize_single_file_download(
        self, client, recorder, mock_metax, not_found_task
    ):
        response = client.post(
            self.endpoint,
            json={"dataset": not_found_task["dataset_id"], "file": "/test1/file1.txt"},
        )
        assert recorder.called == True
        assert response.status_code == 200

    def test_authorize_single_file_download_non_matching_scope(
        self, client, recorder, mock_metax, not_found_task
    ):
        response = client.post(
            self.endpoint,
            json={
                "dataset": not_found_task["dataset_id"],
                "file": "/test1/non-matching.txt",
            },
        )
        assert recorder.called == True
        assert response.status_code == 404

    def test_empty_request_results_in_validation_error(
        self, client, recorder, mock_metax, success_task
    ):
        response = client.post(self.endpoint, json={})

        assert recorder.called == False
        assert response.status_code == 400
        assert "Bad Request" in str(response.get_json())

    def test_cannot_connect_to_metax(self, client, metax_cannot_connect, success_task):
        response = client.post(
            self.endpoint,
            json={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_dataset_cannot_be_found_in_metax(
        self, client, mock_metax, metax_dataset_not_found, success_task
    ):
        response = client.post(
            self.endpoint,
            json={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert response.status_code == 404
        assert "was not found in Metax API" in str(response.data)

    def test_missing_fields_in_metax_response(
        self, client, mock_metax, metax_missing_fields, success_task
    ):
        response = client.post(
            self.endpoint,
            json={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_unexpected_status_code_in_metax_response(
        self, client, mock_metax, metax_unexpected_status_code, success_task
    ):
        response = client.post(
            self.endpoint,
            json={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)


@pytest.mark.usefixtures("mock_metax")
class TestGetDownload:
    endpoint = "/download"

    def test_empty_query_parameters(self, client, recorder):
        response = client.get(self.endpoint, query_string={})
        assert recorder.called == False
        assert response.status_code == 400

    def test_successful_task_with_valid_token_in_header(
        self, client, recorder, success_task, valid_auth_token
    ):
        client.environ_base["HTTP_AUTHORIZATION"] = "Bearer " + valid_auth_token
        query_string = {
            "dataset": success_task["dataset_id"],
            "package": success_task["package"],
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert recorder.called
        assert response.status_code == 200

        # Verify token is single-use
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 401

    def test_single_file_with_valid_token_in_header(
        self, client, recorder, success_task, valid_auth_token_for_file
    ):
        client.environ_base["HTTP_AUTHORIZATION"] = (
            "Bearer " + valid_auth_token_for_file
        )
        query_string = {
            "dataset": success_task["dataset_id"],
            "file": "/test1/file1.txt",
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200

        # Verify token is single-use
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 401

    def test_successful_task_with_valid_token_in_params(
        self, client, recorder, success_task, valid_auth_token
    ):
        query_string = {
            "token": valid_auth_token,
            "dataset": success_task["dataset_id"],
            "package": success_task["package"],
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert recorder.called
        assert response.status_code == 200

        # Verify token is single-use
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 401

    def test_cannot_connect_to_metax(
        self, client, metax_cannot_connect, success_task, valid_auth_token
    ):
        client.environ_base["HTTP_AUTHORIZATION"] = "Bearer " + valid_auth_token
        response = client.get(
            self.endpoint,
            query_string={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_dataset_cannot_be_found_in_metax(
        self,
        client,
        mock_metax,
        metax_dataset_not_found,
        success_task,
        valid_auth_token,
    ):
        client.environ_base["HTTP_AUTHORIZATION"] = "Bearer " + valid_auth_token
        response = client.get(
            self.endpoint,
            query_string={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert response.status_code == 404
        assert "was not found in Metax API" in str(response.data)

    def test_missing_fields_in_metax_response(
        self, client, mock_metax, metax_missing_fields, success_task, valid_auth_token
    ):
        client.environ_base["HTTP_AUTHORIZATION"] = "Bearer " + valid_auth_token
        response = client.get(
            self.endpoint,
            query_string={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_unexpected_status_code_in_metax_response(
        self,
        client,
        mock_metax,
        metax_unexpected_status_code,
        success_task,
        valid_auth_token,
    ):
        client.environ_base["HTTP_AUTHORIZATION"] = "Bearer " + valid_auth_token
        response = client.get(
            self.endpoint,
            query_string={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert response.status_code == 500
        assert "Internal Server Error" in str(response.data)

    def test_download_unauthorized(self, client, success_task):
        query_string = {
            "dataset": success_task["dataset_id"],
            "package": success_task["package"],
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 401

    def test_download_invalid_auth_type(self, client, success_task):
        query_string = {
            "dataset": success_task["dataset_id"],
            "package": success_task["package"],
        }
        client.environ_base["HTTP_AUTHORIZATION"] = "Basic " + "user:pass"
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 401

    def test_download_empty_token(self, client, success_task):
        query_string = {
            "dataset": success_task["dataset_id"],
            "package": success_task["package"],
        }
        client.environ_base["HTTP_AUTHORIZATION"] = "Basic "
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 401

    def test_download_expired_token(self, client, expired_auth_token, success_task):
        client.environ_base["HTTP_AUTHORIZATION"] = "Bearer " + expired_auth_token
        query_string = {
            "dataset": success_task["dataset_id"],
            "package": success_task["package"],
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 401

    def test_download_outdated_generated_package_download(
        self, client, recorder, mock_metax_modified, success_task, valid_auth_token
    ):
        client.environ_base["HTTP_AUTHORIZATION"] = "Bearer " + valid_auth_token
        response = client.get(
            self.endpoint,
            query_string={
                "dataset": success_task["dataset_id"],
                "package": success_task["package"],
            },
        )
        assert recorder.called
        assert response.status_code == 409

    def test_download_non_found_package(
        self, client, not_found_task, not_found_task_auth_token
    ):
        client.environ_base["HTTP_AUTHORIZATION"] = (
            "Bearer " + not_found_task_auth_token
        )
        query_string = {"dataset": not_found_task["dataset_id"], "package": "test.zip"}
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 404

    def test_download_invalid_token(self, client, success_task):
        client.environ_base["HTTP_AUTHORIZATION"] = "Bearer invalid"
        query_string = {
            "dataset": success_task["dataset_id"],
            "package": success_task["package"],
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 401
