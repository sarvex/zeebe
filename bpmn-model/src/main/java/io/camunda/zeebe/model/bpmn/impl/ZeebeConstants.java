/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.model.bpmn.impl;

public class ZeebeConstants {

  public static final String ATTRIBUTE_RETRIES = "retries";
  public static final String ATTRIBUTE_TYPE = "type";

  public static final String ATTRIBUTE_KEY = "key";
  public static final String ATTRIBUTE_NAME = "name";
  public static final String ATTRIBUTE_VALUE = "value";

  public static final String ATTRIBUTE_SOURCE = "source";
  public static final String ATTRIBUTE_TARGET = "target";

  public static final String ATTRIBUTE_CORRELATION_KEY = "correlationKey";
  public static final String ATTRIBUTE_MESSAGE_ID = "messageId";
  public static final String ATTRIBUTE_MESSAGE_TIME_TO_LIVE = "timeToLive";

  public static final String ATTRIBUTE_INPUT_COLLECTION = "inputCollection";
  public static final String ATTRIBUTE_INPUT_ELEMENT = "inputElement";
  public static final String ATTRIBUTE_OUTPUT_COLLECTION = "outputCollection";
  public static final String ATTRIBUTE_OUTPUT_ELEMENT = "outputElement";

  public static final String ATTRIBUTE_PROCESS_ID = "processId";
  public static final String ATTRIBUTE_PROPAGATE_ALL_CHILD_VARIABLES = "propagateAllChildVariables";
  public static final String ATTRIBUTE_PROPAGATE_ALL_PARENT_VARIABLES =
      "propagateAllParentVariables";

  public static final String ATTRIBUTE_FORM_KEY = "formKey";
  public static final String ATTRIBUTE_FORM_ID = "formId";
  public static final String ATTRIBUTE_EXTERNAL_REFERENCE = "externalReference";

  public static final String ATTRIBUTE_ASSIGNEE = "assignee";
  public static final String ATTRIBUTE_CANDIDATE_GROUPS = "candidateGroups";
  public static final String ATTRIBUTE_CANDIDATE_USERS = "candidateUsers";

  public static final String ATTRIBUTE_DUE_DATE = "dueDate";
  public static final String ATTRIBUTE_FOLLOW_UP_DATE = "followUpDate";

  public static final String ATTRIBUTE_DECISION_ID = "decisionId";

  public static final String ATTRIBUTE_EXPRESSION = "expression";

  public static final String ATTRIBUTE_RESULT_VARIABLE = "resultVariable";

  public static final String ELEMENT_HEADER = "header";
  public static final String ELEMENT_INPUT = "input";
  public static final String ELEMENT_IO_MAPPING = "ioMapping";
  public static final String ELEMENT_OUTPUT = "output";

  public static final String ELEMENT_SCRIPT = "script";
  public static final String ELEMENT_SUBSCRIPTION = "subscription";
  public static final String ELEMENT_PUBLISH_MESSAGE = "publishMessage";

  public static final String ELEMENT_TASK_DEFINITION = "taskDefinition";
  public static final String ELEMENT_TASK_HEADERS = "taskHeaders";

  public static final String ELEMENT_FORM_DEFINITION = "formDefinition";
  public static final String ELEMENT_USER_TASK_FORM = "userTaskForm";

  public static final String ELEMENT_ASSIGNMENT_DEFINITION = "assignmentDefinition";

  public static final String ELEMENT_SCHEDULE_DEFINITION = "taskSchedule";

  public static final String ELEMENT_LOOP_CHARACTERISTICS = "loopCharacteristics";

  public static final String ELEMENT_CALLED_ELEMENT = "calledElement";

  public static final String ELEMENT_CALLED_DECISION = "calledDecision";

  public static final String ELEMENT_PROPERTIES = "properties";

  public static final String ELEMENT_PROPERTY = "property";

  public static final String ELEMENT_USER_TASK = "userTask";

  /** Form key format used for camunda-forms format */
  public static final String USER_TASK_FORM_KEY_CAMUNDA_FORMS_FORMAT = "camunda-forms";

  /** Form key location used for forms embedded in the same BPMN file, i.e. zeebeUserTaskForm */
  public static final String USER_TASK_FORM_KEY_BPMN_LOCATION = "bpmn";
}
