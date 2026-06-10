// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import websubhub.common;
import websubhub.config;
import websubhub.connections as conn;

import ballerina/http;
import ballerina/lang.runtime;
import ballerina/lang.value;
import ballerina/log;
import ballerina/websubhub;

import wso2/messagestore.api as storeapi;

// Initial delay (seconds) before the first restart of the hub-state update worker.
// Doubles on each consecutive failure, capped at HUB_STATE_WORKER_MAX_RETRY_DELAY.
const decimal HUB_STATE_WORKER_INITIAL_RETRY_DELAY = 5.0;
const decimal HUB_STATE_WORKER_MAX_RETRY_DELAY = 60.0;

function initializeHubState() returns error? {
    http:Client stateSnapshot;
    common:HttpClientConfig? config = config:state.snapshot.config;
    if config is common:HttpClientConfig {
        http:ClientConfiguration clientConfig = {
            timeout: config.timeout,
            retryConfig: config?.'retry,
            secureSocket: config.secureSocket
        };
        stateSnapshot = check new (config:state.snapshot.url, clientConfig);
    } else {
        stateSnapshot = check new (config:state.snapshot.url);
    }
    do {
        common:SystemStateSnapshot systemStateSnapshot = check stateSnapshot->/;
        processWebsubTopicsSnapshotState(systemStateSnapshot.topics);
        check processWebsubSubscriptionsSnapshotState(systemStateSnapshot.subscriptions);
        // Start hub-state update worker — wrapped in a restart loop so that transient
        // broker failures (e.g. a frozen TCP connection) do not permanently stop the hub
        // from seeing state-change events.
        _ = start startHubStateUpdateWorker();
    } on fail error httpError {
        common:logFatalError("Error occurred while initializing the hub-state using the latest state-snapshot", httpError);
        return httpError;
    }
}

# Starts the hub-state update worker in a resilient restart loop. On any failure the worker
# logs an ERROR, closes the consumer, waits with exponential backoff (capped at
# HUB_STATE_WORKER_MAX_RETRY_DELAY seconds), creates a fresh consumer, and retries. This
# prevents a transient broker connection failure (e.g. a frozen cloud TCP link) from
# permanently stopping the hub from processing state-change events.
function startHubStateUpdateWorker() {
    decimal delay = HUB_STATE_WORKER_INITIAL_RETRY_DELAY;
    while true {
        storeapi:Consumer|error eventsConsumer = conn:createWebSubEventsConsumer();
        if eventsConsumer is error {
            log:printError("Failed to initialize hub-state events consumer, retrying after delay",
                'error = eventsConsumer, retryDelaySeconds = delay);
            runtime:sleep(delay);
            if delay < HUB_STATE_WORKER_MAX_RETRY_DELAY {
                delay = delay * 2.0d;
            }
            continue;
        }
        // Reset backoff on a successful consumer creation.
        delay = HUB_STATE_WORKER_INITIAL_RETRY_DELAY;
        error? result = updateHubState(eventsConsumer);
        if result is error {
            log:printError("Hub-state update worker terminated unexpectedly, restarting after delay",
                'error = result, retryDelaySeconds = delay);
        } else {
            // updateHubState should never return () under normal operation.
            log:printWarn("Hub-state update worker exited without error, restarting");
        }
        runtime:sleep(delay);
        if delay < HUB_STATE_WORKER_MAX_RETRY_DELAY {
            delay = delay * 2.0d;
        }
    }
}

# Runs the hub-state event consume loop using the provided consumer. Returns an error if the
# loop terminates due to a broker failure; the caller (startHubStateUpdateWorker) is
# responsible for closing the consumer and retrying with a fresh connection.
#
# + eventsConsumer - The consumer to use for reading hub-state events
# + return - An error if the consume loop fails, or `()` on unexpected clean exit
function updateHubState(storeapi:Consumer eventsConsumer) returns error? {
    do {
        while true {
            storeapi:Message? message = check eventsConsumer->receive();
            if message is () {
                continue;
            }

            string lastPersistedData = check string:fromBytes(message.payload);
            error? result = processStateUpdateEvent(lastPersistedData);
            if result is error {
                common:logFatalError("Error occurred while processing state-update event", result);
                check eventsConsumer->nack(message);
                check result;
            } else {
                check eventsConsumer->ack(message);
            }
        }
    } on fail error e {
        check eventsConsumer->close();
        return e;
    }
}

function processStateUpdateEvent(string persistedData) returns error? {
    json event = check value:fromJsonString(persistedData);
    string hubMode = check event.hubMode;
    match event.hubMode {
        "register" => {
            websubhub:TopicRegistration topicRegistration = check event.fromJsonWithType();
            processTopicRegistration(topicRegistration);
        }
        "deregister" => {
            websubhub:TopicDeregistration topicDeregistration = check event.fromJsonWithType();
            processTopicDeregistration(topicDeregistration);
        }
        "subscribe" => {
            websubhub:VerifiedSubscription subscription = check event.fromJsonWithType();
            check processSubscription(subscription);
        }
        "unsubscribe" => {
            websubhub:VerifiedUnsubscription unsubscription = check event.fromJsonWithType();
            check processUnsubscription(unsubscription);
        }
        _ => {
            return error(string `Error occurred while deserializing state-update events with invalid hubMode [${hubMode}]`);
        }
    }
}
