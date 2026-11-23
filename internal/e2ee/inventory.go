package e2ee

import pb "github.com/zever4/Zchat/gen/e2ee"

func ValidateInventory(req *pb.GetPreKeyInventoryRequest) error {
	if req == nil {
		return invalid("inventory request")
	}
	if err := ValidateContext(req.Context); err != nil {
		return err
	}
	if len(req.ProtoReflect().GetUnknown()) != 0 || len(req.Context.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown inventory fields")
	}
	return wireSize(req)
}
