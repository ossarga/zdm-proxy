package cloudgateproxy

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datacodec"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/google/uuid"
)

type ParameterModifier struct {
	timeUuidGenerator TimeUuidGenerator
}

func NewParameterModifier(generator TimeUuidGenerator) *ParameterModifier {
	return &ParameterModifier{timeUuidGenerator: generator}
}

// AddValuesToExecuteFrame generates and adds values for function calls that were replaced in the prior PREPARE message.
//
// These values are added as positional or named depending on whether the frame already has positional
// or named values in the message.
//
// If no values exist in the frame, then this method adds the new values as positional by default
// because they are more efficient.
//
// The *message.Execute object that is returned is the same object that the provided frame contains,
// no new frames or messages are generated by this function.
func (recv *ParameterModifier) AddValuesToExecuteFrame(
	newFrame *frame.Frame, prepareRequestInfo *PrepareRequestInfo,
	variablesMetadata *message.VariablesMetadata,
	replacementTimeUuids []*uuid.UUID) (*message.Execute, error) {
	newExecuteMsg, ok := newFrame.Body.Message.(*message.Execute)
	if !ok {
		return nil, fmt.Errorf("expected Execute but got %v instead", newFrame.Body.Message.GetOpCode())
	}

	if len(prepareRequestInfo.GetReplacedTerms()) > 0 {
		err := recv.addValuesToExecuteMessage(
			newFrame.Header.Version, newExecuteMsg, prepareRequestInfo, variablesMetadata, replacementTimeUuids)
		if err != nil {
			return nil, err
		}
	}

	return newExecuteMsg, nil
}

func (recv *ParameterModifier) addValuesToExecuteMessage(
	version primitive.ProtocolVersion, executeMsg *message.Execute,
	prepareRequestInfo *PrepareRequestInfo, variablesMetadata *message.VariablesMetadata,
	replacementTimeUuids []*uuid.UUID) error {
	if executeMsg.Options == nil {
		executeMsg.Options = &message.QueryOptions{}
	}

	var err error
	if len(executeMsg.Options.NamedValues) == 0 {
		var newPositionalValues []*primitive.Value
		newPositionalValues, err = recv.addPositionalValuesForReplacedTerms(
			version, executeMsg.Options.PositionalValues, prepareRequestInfo, variablesMetadata, replacementTimeUuids)
		if err == nil {
			executeMsg.Options.PositionalValues = newPositionalValues
		}
	} else {
		err = recv.addNamedValuesForReplacedNamedMarkers(version, executeMsg, variablesMetadata, replacementTimeUuids)
	}

	return err
}

func (recv *ParameterModifier) addValuesToBatchChild(
	version primitive.ProtocolVersion, batchChild *message.BatchChild,
	prepareRequestInfo *PrepareRequestInfo, variablesMetadata *message.VariablesMetadata,
	replacementTimeUuids []*uuid.UUID) error {

	newPositionalValues, err := recv.addPositionalValuesForReplacedTerms(
		version, batchChild.Values, prepareRequestInfo, variablesMetadata, replacementTimeUuids)
	if err == nil {
		batchChild.Values = newPositionalValues
	}

	return err
}

func (recv *ParameterModifier) generateTimeUuids(prepareRequestInfo *PrepareRequestInfo) []*uuid.UUID {
	generatedUuids := make([]*uuid.UUID, 0, len(prepareRequestInfo.GetReplacedTerms()))
	for _, currentTerm := range prepareRequestInfo.GetReplacedTerms() {
		if currentTerm.isFunctionCall() == currentTerm.functionCall.isNow() {
			newUuid := recv.timeUuidGenerator.GetTimeUuid()
			generatedUuids = append(generatedUuids, &newUuid)
		}
	}
	return generatedUuids
}

func (recv *ParameterModifier) addPositionalValuesForReplacedTerms(
	version primitive.ProtocolVersion, originalPositionalValues []*primitive.Value,
	prepareRequestInfo *PrepareRequestInfo, variablesMetadata *message.VariablesMetadata,
	replacementTimeUuids []*uuid.UUID) ([]*primitive.Value, error) {
	if prepareRequestInfo.ContainsPositionalMarkers() {
		return recv.addPositionalValuesForReplacedPositionalMarkers(
			version, originalPositionalValues, prepareRequestInfo.GetReplacedTerms(), variablesMetadata, replacementTimeUuids)
	} else {
		return recv.addPositionalValuesForReplacedNamedMarkers(version, originalPositionalValues, variablesMetadata, replacementTimeUuids)
	}
}

func (recv *ParameterModifier) addPositionalValuesForReplacedPositionalMarkers(version primitive.ProtocolVersion,
	originalPositionalValues []*primitive.Value, replacedTerms []*term, variablesMetadata *message.VariablesMetadata,
	replacementTimeUuids []*uuid.UUID) ([]*primitive.Value, error) {
	newPositionalValues := make([]*primitive.Value, 0, len(originalPositionalValues) + len(replacedTerms))
	start := 0
	offset := 0
	replacementIdx := 0
	for _, currentTerm := range replacedTerms {
		newValueIdx := offset + currentTerm.previousPositionalIndex + 1
		if currentTerm.previousPositionalIndex >= len(originalPositionalValues) {
			return nil, fmt.Errorf("current term has previous positional index %v but " +
				"number of positional values in the request is %v",
				currentTerm.previousPositionalIndex, len(originalPositionalValues))
		}

		if currentTerm.previousPositionalIndex >= 0 {
			end := currentTerm.previousPositionalIndex + 1
			newPositionalValues = append(
				newPositionalValues,
				originalPositionalValues[start:end]...)
			start = end
		}

		if currentTerm.isFunctionCall() && currentTerm.functionCall.isNow() {
			if newValueIdx >= len(variablesMetadata.Columns) {
				return nil, fmt.Errorf("could not insert positional value (%v) because columns metadata " +
					"has unexpected length; variablesmetadata: %v", newValueIdx, variablesMetadata)
			}
			if replacementIdx >= len(replacementTimeUuids) {
				return nil, fmt.Errorf("could not replace positional value (%v) with index %v because replacement timeuuids " +
					"has unexpected length: %v", newValueIdx, replacementIdx, replacementTimeUuids)
			}

			generatedTimeUuidValue, err := recv.generateTimeUuidValue(
				replacementTimeUuids[replacementIdx], version, variablesMetadata.Columns[newValueIdx].Type)
			if err != nil {
				return nil, fmt.Errorf("could not generate new timeuuid value: %w", err)
			}
			replacementIdx++

			newPositionalValues = append(newPositionalValues, generatedTimeUuidValue)
			offset++
		}
	}

	if start < len(originalPositionalValues) {
		newPositionalValues = append(
			newPositionalValues,
			originalPositionalValues[start:]...)
	}
	return newPositionalValues, nil
}

func (recv *ParameterModifier) addPositionalValuesForReplacedNamedMarkers(version primitive.ProtocolVersion,
	originalPositionalValues []*primitive.Value, variablesMetadata *message.VariablesMetadata, replacementTimeUuids []*uuid.UUID) ([]*primitive.Value, error) {
	colLength := len(variablesMetadata.Columns)
	originalPositionalValuesLength := len(originalPositionalValues)
	newPositionalValues := make([]*primitive.Value, 0, colLength)

	replacementIdx := 0
	currentIdx := 0
	for _, col := range variablesMetadata.Columns {
		replaced := false
		if col.Name != "" {
			switch col.Name {
			case cloudgateNowNamedMarker:
				if replacementIdx >= len(replacementTimeUuids) {
					return nil, fmt.Errorf("could not replace positional value with index %v because replacement timeuuids " +
						"has unexpected length: %v", replacementIdx, replacementTimeUuids)
				}
				generatedTimeUuidValue, err := recv.generateTimeUuidValue(replacementTimeUuids[replacementIdx], version, col.Type)
				if err != nil {
					return nil, fmt.Errorf("could not generate new timeuuid value: %w", err)
				}
				replacementIdx++
				newPositionalValues = append(newPositionalValues, generatedTimeUuidValue)
				replaced = true
			default:
			}
		}

		if !replaced {
			if currentIdx >= originalPositionalValuesLength {
				return nil, fmt.Errorf("invalid index (%v) while copying positional values: %v", currentIdx, originalPositionalValuesLength)
			}
			newPositionalValues = append(newPositionalValues, originalPositionalValues[currentIdx])
			currentIdx++
		}
	}

	return newPositionalValues, nil
}

func (recv *ParameterModifier) addNamedValuesForReplacedNamedMarkers(version primitive.ProtocolVersion,
	executeMsg *message.Execute, variablesMetadata *message.VariablesMetadata, replacementTimeUuids []*uuid.UUID) error {
	if executeMsg.Options.NamedValues == nil {
		executeMsg.Options.NamedValues = make(map[string]*primitive.Value, len(variablesMetadata.Columns))
	}

	replacementIdx := 0
	for _, col := range variablesMetadata.Columns {
		if col.Name != "" {
			_, exists := executeMsg.Options.NamedValues[col.Name]
			if exists {
				continue
			}

			switch col.Name {
			case cloudgateNowNamedMarker:
				if replacementIdx >= len(replacementTimeUuids) {
					return fmt.Errorf("could not replace named value (%v) with index (%v) because " +
						"replacement timeuuids has unexpected length: %v",
						cloudgateNowNamedMarker, replacementIdx, replacementTimeUuids)
				}
				generatedTimeUuidValue, err := recv.generateTimeUuidValue(replacementTimeUuids[replacementIdx], version, col.Type)
				if err != nil {
					return err
				}
				replacementIdx++
				executeMsg.Options.NamedValues[cloudgateNowNamedMarker] = generatedTimeUuidValue
			default:
				return fmt.Errorf("could not generate value for column %v", col.Name)
			}
		}
	}

	return nil
}

func (recv *ParameterModifier) generateTimeUuidValue(
	timeUuid *uuid.UUID, version primitive.ProtocolVersion, valueType datatype.DataType) (*primitive.Value, error) {
	newValueCodec, err := datacodec.NewCodec(valueType)
	if err != nil {
		return nil, fmt.Errorf("could not create codec for new timeuuid value: %w", err)
	}

	generatedTimeUuidPrimitive := primitive.UUID(*timeUuid)
	generatedTimeUuidValue, err := newValueCodec.Encode(&generatedTimeUuidPrimitive, version)
	if err != nil {
		return nil, fmt.Errorf("could not encode new timeuuid value: %w", err)
	}

	return primitive.NewValue(generatedTimeUuidValue), nil
}