import MigrationTypes "../types";
import Time "mo:base/Time";
import v0_1_0 "types";
import D "mo:base/Debug";
import Text "mo:base/Text";
import ICRC72Publisher "../../../../icrc72-publisher.mo/src/";

module {
  public type State = MigrationTypes.State;

  public let BTree = v0_1_0.BTree;
  public let Map = v0_1_0.Map;
  public let Set = v0_1_0.Set;
  public let Vector = v0_1_0.Vector;

  public func publisherInitialState() : State {#v0_0_0(#data)};
  public let publisherCurrentStateVersion = #v0_1_0(#id);

  public func upgrade(prevmigration_state: MigrationTypes.State, args: MigrationTypes.Args, caller: Principal): MigrationTypes.State {

    let (name) = switch (args) {
      case (?args) {(
        args.name)};
      case (_) {("nobody")};
    };

    let state : v0_1_0.State = {
      publications = BTree.init<Nat, v0_1_0.PublicationRecord>(null);
      publicationsByNamespace = BTree.init<Text, Nat>(null);
      broadcasters = BTree.init<Principal, v0_1_0.BroadcasterRecord>(null);
      broadcastersBySubnet = Map.new<Principal, Vector.Vector<Principal>>();
      var nextPublicationID = 0;
      var nextSubscriptionID = 0;
      subscriptions = BTree.init<Nat, v0_1_0.SubscriptionRecord>(null);
      subscriptionsByNamespace = BTree.init<Text, v0_1_0.SubscriptionIndex>(null);
      subscribersByPrincipal = BTree.init<Principal, Set.Set<Nat>>(null);
      var maxTake = 100;
      var defaultTake = 100;
    };

    return #v0_1_0(#data(state));
  };
};