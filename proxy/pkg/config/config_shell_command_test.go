package config

import (
	"os/exec"
	"testing"
)

func TestExecuteShellCommand_NoCommand(t *testing.T) {
	config := &Config{}
	err := config.executeShellCommand()
	if err != nil {
		t.Errorf("Expected no error when no shell command is defined, got: %v", err)
	}
}

func TestExecuteShellCommand_TextPlain_SimpleEcho(t *testing.T) {
	// Skip this test on systems where sh is not available
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not found in PATH, skipping shell command test")
	}

	config := &Config{
		ShellCommand:           "echo 'password1\npassword2'",
		ShellCommandTimeoutMs:  5000,
		ShellResponseMapping:   `{"1": "OriginPassword", "2": "TargetPassword"}`,
		ShellResponseDelimiter: "\n",
		ShellResponseMimeType:  "text/plain",
	}

	err := config.executeShellCommand()
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if config.OriginPassword != "password1" {
		t.Errorf("Expected OriginPassword to be 'password1', got: %s", config.OriginPassword)
	}

	if config.TargetPassword != "password2" {
		t.Errorf("Expected TargetPassword to be 'password2', got: %s", config.TargetPassword)
	}
}

func TestExecuteShellCommand_JSON_Response(t *testing.T) {
	// Skip this test on systems where sh is not available
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not found in PATH, skipping shell command test")
	}

	config := &Config{
		ShellCommand:          `echo '{"data":{"data":{"avaloq_password":"secret1","spark_password":"secret2"}}}'`,
		ShellCommandTimeoutMs: 5000,
		ShellResponseMapping:  `{"data.data.avaloq_password": "OriginPassword", "data.data.spark_password": "TargetPassword"}`,
		ShellResponseMimeType: "application/json",
	}

	err := config.executeShellCommand()
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if config.OriginPassword != "secret1" {
		t.Errorf("Expected OriginPassword to be 'secret1', got: %s", config.OriginPassword)
	}

	if config.TargetPassword != "secret2" {
		t.Errorf("Expected TargetPassword to be 'secret2', got: %s", config.TargetPassword)
	}
}

func TestExecuteShellCommand_InvalidMimeType(t *testing.T) {
	config := &Config{
		ShellCommand:          "echo 'test'",
		ShellCommandTimeoutMs: 5000,
		ShellResponseMapping:  `{"1": "OriginPassword"}`,
		ShellResponseMimeType: "invalid/type",
	}

	err := config.executeShellCommand()
	if err == nil {
		t.Error("Expected error for invalid MIME type, got nil")
	}
}

func TestParseShellResponseMapping_TextPlain_Valid(t *testing.T) {
	config := &Config{
		ShellResponseMapping:  `{"1": "OriginPassword", "2": "TargetPassword"}`,
		ShellResponseMimeType: "text/plain",
	}

	mapping, err := config.parseShellResponseMapping(config.parseTextPlainMapping)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	textMapping, ok := mapping.(map[int]string)
	if !ok {
		t.Errorf("Expected map[int]string, got %T", mapping)
	}

	if textMapping[1] != "OriginPassword" {
		t.Errorf("Expected mapping[1] to be 'OriginPassword', got: %s", textMapping[1])
	}

	if textMapping[2] != "TargetPassword" {
		t.Errorf("Expected mapping[2] to be 'TargetPassword', got: %s", textMapping[2])
	}
}

func TestParseShellResponseMapping_JSON_Valid(t *testing.T) {
	config := &Config{
		ShellResponseMapping:  `{"data.data.avaloq_password": "OriginPassword", "data.data.spark_password": "TargetPassword"}`,
		ShellResponseMimeType: "application/json",
	}

	mapping, err := config.parseShellResponseMapping(config.parseJSONMapping)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	jsonMapping, ok := mapping.(map[string]string)
	if !ok {
		t.Errorf("Expected map[string]string, got %T", mapping)
	}

	if jsonMapping["data.data.avaloq_password"] != "OriginPassword" {
		t.Errorf("Expected mapping for 'data.data.avaloq_password' to be 'OriginPassword', got: %s", jsonMapping["data.data.avaloq_password"])
	}

	if jsonMapping["data.data.spark_password"] != "TargetPassword" {
		t.Errorf("Expected mapping for 'data.data.spark_password' to be 'TargetPassword', got: %s", jsonMapping["data.data.spark_password"])
	}
}

func TestParseShellResponseMapping_TextPlain_InvalidPosition(t *testing.T) {
	config := &Config{
		ShellResponseMapping:  `{"0": "OriginPassword"}`, // Position 0 is invalid
		ShellResponseMimeType: "text/plain",
	}

	_, err := config.parseShellResponseMapping(config.parseTextPlainMapping)
	if err == nil {
		t.Error("Expected error for position 0, got nil")
	}
}

func TestExtractJSONValue_SimpleObject(t *testing.T) {
	config := &Config{}
	data := map[string]interface{}{
		"data": map[string]interface{}{
			"password": "secret123",
			"port":     float64(9042),
			"enabled":  true,
		},
	}

	// Test string extraction
	value, err := config.extractJSONValue(data, "data.password")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if value != "secret123" {
		t.Errorf("Expected 'secret123', got: %s", value)
	}

	// Test number extraction
	value, err = config.extractJSONValue(data, "data.port")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if value != "9042" {
		t.Errorf("Expected '9042', got: %s", value)
	}

	// Test boolean extraction
	value, err = config.extractJSONValue(data, "data.enabled")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if value != "true" {
		t.Errorf("Expected 'true', got: %s", value)
	}
}

func TestExtractJSONValue_NonExistentPath(t *testing.T) {
	config := &Config{}
	data := map[string]interface{}{
		"data": map[string]interface{}{
			"password": "secret123",
		},
	}

	_, err := config.extractJSONValue(data, "data.nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent path, got nil")
	}
}

func TestSetConfigProperty_String(t *testing.T) {
	config := &Config{}
	err := config.setConfigProperty("OriginPassword", "testpassword")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if config.OriginPassword != "testpassword" {
		t.Errorf("Expected OriginPassword to be 'testpassword', got: %s", config.OriginPassword)
	}
}

func TestSetConfigProperty_Int(t *testing.T) {
	config := &Config{}
	err := config.setConfigProperty("OriginPort", "9999")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if config.OriginPort != 9999 {
		t.Errorf("Expected OriginPort to be 9999, got: %d", config.OriginPort)
	}
}

func TestSetConfigProperty_Bool(t *testing.T) {
	config := &Config{}
	err := config.setConfigProperty("ReplaceCqlFunctions", "true")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if !config.ReplaceCqlFunctions {
		t.Error("Expected ReplaceCqlFunctions to be true")
	}
}

func TestSetConfigProperty_InvalidField(t *testing.T) {
	config := &Config{}
	err := config.setConfigProperty("NonExistentField", "value")
	if err == nil {
		t.Error("Expected error for non-existent field, got nil")
	}
}

func TestIsValidConfigProperty(t *testing.T) {
	config := &Config{}

	if !config.isValidConfigProperty("OriginPassword") {
		t.Error("Expected OriginPassword to be valid")
	}

	if config.isValidConfigProperty("NonExistentField") {
		t.Error("Expected NonExistentField to be invalid")
	}
}

// TestShellCommand_CompleteWorkflow tests the complete workflow with both MIME types
func TestShellCommand_CompleteWorkflow(t *testing.T) {
	// Skip this test on systems where sh is not available
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not found in PATH, skipping shell command test")
	}

	// Test 1: text/plain with custom delimiter
	t.Run("TextPlain_CustomDelimiter", func(t *testing.T) {
		config := &Config{
			ShellCommand:           "echo 'user1|pass1|9042'",
			ShellCommandTimeoutMs:  5000,
			ShellResponseMapping:   `{"1": "OriginUsername", "2": "OriginPassword", "3": "OriginPort"}`,
			ShellResponseDelimiter: "|",
			ShellResponseMimeType:  "text/plain",
		}

		err := config.executeShellCommand()
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if config.OriginUsername != "user1" {
			t.Errorf("Expected OriginUsername to be 'user1', got: %s", config.OriginUsername)
		}
		if config.OriginPassword != "pass1" {
			t.Errorf("Expected OriginPassword to be 'pass1', got: %s", config.OriginPassword)
		}
		if config.OriginPort != 9042 {
			t.Errorf("Expected OriginPort to be 9042, got: %d", config.OriginPort)
		}
	})

	// Test 2: application/json with nested paths
	t.Run("JSON_NestedPaths", func(t *testing.T) {
		config := &Config{
			ShellCommand:          `echo '{"credentials":{"origin":{"user":"originuser","pass":"originpass"},"target":{"user":"targetuser","pass":"targetpass"}},"settings":{"enabled":true}}'`,
			ShellCommandTimeoutMs: 5000,
			ShellResponseMapping:  `{"credentials.origin.user": "OriginUsername", "credentials.origin.pass": "OriginPassword", "credentials.target.user": "TargetUsername", "credentials.target.pass": "TargetPassword", "settings.enabled": "ReplaceCqlFunctions"}`,
			ShellResponseMimeType: "application/json",
		}

		err := config.executeShellCommand()
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if config.OriginUsername != "originuser" {
			t.Errorf("Expected OriginUsername to be 'originuser', got: %s", config.OriginUsername)
		}
		if config.OriginPassword != "originpass" {
			t.Errorf("Expected OriginPassword to be 'originpass', got: %s", config.OriginPassword)
		}
		if config.TargetUsername != "targetuser" {
			t.Errorf("Expected TargetUsername to be 'targetuser', got: %s", config.TargetUsername)
		}
		if config.TargetPassword != "targetpass" {
			t.Errorf("Expected TargetPassword to be 'targetpass', got: %s", config.TargetPassword)
		}
		if !config.ReplaceCqlFunctions {
			t.Error("Expected ReplaceCqlFunctions to be true")
		}
	})
}
