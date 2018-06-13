package planfile

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	"github.com/zclconf/go-cty/cty"
	ctymsgpack "github.com/zclconf/go-cty/cty/msgpack"

	"github.com/hashicorp/terraform/plans"
	"github.com/hashicorp/terraform/plans/internal/planproto"
	"github.com/hashicorp/terraform/version"
)

const tfplanFormatVersion = 3

// ---------------------------------------------------------------------------
// This file deals with the internal structure of the "tfplan" sub-file within
// the plan file format. It's all private API, wrapped by methods defined
// elsewhere. This is the only file that should import the
// ../internal/planproto package, which contains the ugly stubs generated
// by the protobuf compiler.
// ---------------------------------------------------------------------------

// readTFPlan reads a protobuf-encoded description from the plan portion of
// a plan file, which is stored in a special file in the archive called
// "tfplan".
func readTFPlan(r io.Reader) (*plans.Plan, error) {
	src, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var rawPlan planproto.Plan
	err = proto.Unmarshal(src, &rawPlan)
	if err != nil {
		return nil, fmt.Errorf("parse error: %s", err)
	}

	if rawPlan.Version != tfplanFormatVersion {
		return nil, fmt.Errorf("unsupported plan file format version %d; only version %d is supported", rawPlan.Version, tfplanFormatVersion)
	}

	if rawPlan.TerraformVersion != version.String() {
		return nil, fmt.Errorf("plan file was created by Terraform %s, but this is %s; plan files cannot be transferred between different Terraform versions", rawPlan.TerraformVersion, version.String())
	}

	// TODO: Populate the rest of this!
	plan := &plans.Plan{
		VariableValues: map[string]cty.Value{},
		Changes: &plans.Changes{
			RootOutputs: map[string]*plans.OutputChange{},
			Resources:   []*plans.ResourceInstanceChange{},
		},

		ProviderSHA256s: map[string][]byte{},
	}

	return plan, nil
}

// writeTFPlan serializes the given plan into the protobuf-based format used
// for the "tfplan" portion of a plan file.
func writeTFPlan(plan *plans.Plan, w io.Writer) error {
	rawPlan := &planproto.Plan{
		Version:          tfplanFormatVersion,
		TerraformVersion: version.String(),
		ProviderHashes:   map[string]*planproto.Hash{},

		Variables:       map[string]*planproto.DynamicValue{},
		OutputChanges:   []*planproto.OutputChange{},
		ResourceChanges: []*planproto.ResourceInstanceChange{},
	}

	for name, oc := range plan.Changes.RootOutputs {
		// Writing outputs as cty.DynamicPseudoType forces the stored values
		// to also contain dynamic type information, so we can recover the
		// original type when we read the values back in readTFPlan.
		protoChange, err := changeToTfplan(&oc.Change, cty.DynamicPseudoType)
		if err != nil {
			return fmt.Errorf("cannot write output value %q: %s", name, err)
		}

		rawPlan.OutputChanges = append(rawPlan.OutputChanges, &planproto.OutputChange{
			Name:      name,
			Change:    protoChange,
			Sensitive: oc.Sensitive,
		})
	}

	src, err := proto.Marshal(rawPlan)
	if err != nil {
		return fmt.Errorf("serialization error: %s", err)
	}

	_, err = w.Write(src)
	if err != nil {
		return fmt.Errorf("failed to write plan to plan file: %s", err)
	}

	return nil
}

func changeToTfplan(change *plans.Change, valueTy cty.Type) (*planproto.Change, error) {
	ret := &planproto.Change{}

	var before, after *planproto.DynamicValue
	if change.Before != cty.NilVal {
		var err error
		before, err = valueToTfplan(change.Before, valueTy)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize 'before' value: %s", err)
		}
	}
	if change.Before != cty.NilVal {
		var err error
		after, err = valueToTfplan(change.After, valueTy)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize 'after' value: %s", err)
		}
	}

	switch change.Action {
	case plans.Create:
		ret.Action = planproto.Action_CREATE
		ret.Values = []*planproto.DynamicValue{after}
	case plans.Read:
		ret.Action = planproto.Action_READ
		ret.Values = []*planproto.DynamicValue{before, after}
	case plans.Update:
		ret.Action = planproto.Action_UPDATE
		ret.Values = []*planproto.DynamicValue{before, after}
	case plans.Replace:
		ret.Action = planproto.Action_REPLACE
		ret.Values = []*planproto.DynamicValue{before, after}
	case plans.Delete:
		ret.Action = planproto.Action_DELETE
		ret.Values = []*planproto.DynamicValue{before}
	default:
		return nil, fmt.Errorf("invalid change action %s", change.Action)
	}

	return ret, nil
}

func valueToTfplan(val cty.Value, ty cty.Type) (*planproto.DynamicValue, error) {
	buf, err := ctymsgpack.Marshal(val, ty)
	if err != nil {
		return nil, err
	}

	return &planproto.DynamicValue{
		Msgpack: buf,
	}, nil
}
