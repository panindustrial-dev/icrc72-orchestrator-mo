module {
  public let CANISTER_ID = "rwlgt-iiaaa-aaaaa-aaaaa-cai";
  public type GetSubnetForCanisterRequest = { principal : ?Principal };
  public type GetSubnetForCanisterResponse = {
    #Ok : { subnet_id : ?Principal };
    #Err : Text;
  };
  public type Service = actor {
    get_build_metadata : shared query () -> async Text;
    get_subnet_for_canister : shared query GetSubnetForCanisterRequest -> async GetSubnetForCanisterResponse;
  }
}