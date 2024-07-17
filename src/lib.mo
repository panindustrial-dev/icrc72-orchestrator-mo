import MigrationTypes "migrations/types";
import Migration "migrations";
import Service "service";
import BTree "mo:stableheapbtreemap/BTree";
import Map "mo:map/Map";
import Array "mo:base/Array";
import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Star "mo:star/star";
import Text "mo:base/Text";
import TT "../../timerTool/src/";
import ICRC75Service = "../../ICRC75/src/service";
import D "mo:base/Debug";
import Error "mo:base/Error";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Conversion = "mo:candy/conversion";
import Candy = "mo:candy/types";

module {

  public type State = MigrationTypes.State;

  public type CurrentState = MigrationTypes.Current.State;

  public type Environment = MigrationTypes.Current.Environment;
  public type PermissionSet = MigrationTypes.Current.PermissionSet;
  public type PublisherRecord = MigrationTypes.Current.PublisherRecord;
  public type Value = MigrationTypes.Current.Value;
  public type ICRC16 = MigrationTypes.Current.ICRC16;
  public type ICRC16Map = MigrationTypes.Current.ICRC16Map;
  public type Namespace = MigrationTypes.Current.Namespace;
  public type PublicationRecord = MigrationTypes.Current.PublicationRecord;
  public type SubscriptionRecord = MigrationTypes.Current.SubscriptionRecord;
  public type BroadcasterRecord = MigrationTypes.Current.BroadcasterRecord;
  public type SubscriberRecord = MigrationTypes.Current.SubscriberRecord;
              
  public type CanAddPublication = MigrationTypes.Current.CanAddPublication;
  public type CanAddSubscription = MigrationTypes.Current.CanAddSubscription;
  public type CanUpdatePublication = MigrationTypes.Current.CanUpdatePublication;
  public type CanUpdateSubscription = MigrationTypes.Current.CanUpdateSubscription;
  public type PublicationRegisteredListener = MigrationTypes.Current.PublicationRegisteredListener;
  public type SubscriptionRegisteredListener = MigrationTypes.Current.SubscriptionRegisteredListener;
  public type SubscriberRegisteredListener = MigrationTypes.Current.SubscriberRegisteredListener;
  public type ICRC75Item = MigrationTypes.Current.ICRC75Item;
  public type GenericError = MigrationTypes.Current.GenericError;

  public type PublicationRegistration = Service.PublicationRegistration;
  public type PublicationRegisterResult = Service.PublicationRegisterResult;
  public type PublicationUpdateRequest = Service.PublicationUpdateRequest;
  public type PublicationUpdateResult = Service.PublicationUpdateResult;
  public type SubscriptionRegistration = Service.SubscriptionRegistration;
  public type SubscriptionRegisterResult = Service.SubscriptionRegisterResult;
  public type SubscriptionRegisterError = Service.SubscriptionRegisterError;

  public type SubscriptionUpdateRequest = Service.SubscriptionUpdateRequest;
  public type SubscriptionUpdateResult = Service.SubscriptionUpdateResult;
  public type SubscriptionUpdateError = Service.SubscriptionUpdateError;
  public type PublisherInfoResponse = Service.PublisherInfo;
  public type PublicationInfoResponse = Service.PublicationInfo;
  public type PublisherPublicationInfoResponse = Service.PublisherPublicationInfo;
  public type SubscriberInfoResponse = Service.SubscriberInfo;
  public type SubscriberSubscriptionInfoResponse = Service.SubscriberSubscriptionInfo;
  public type SubscriptionInfoResponse = Service.SubscriptionInfo;
  public type BroadcasterInfoResponse = Service.BroadcasterInfo;
  public type StatsResponse = Service.Stats;
  public type PublicationIdentifier = Service.PublicationIdentifier;
  public type SubscriptionIdentifier = Service.SubscriptionIdentifier;

  
  public let init = Migration.migrate;
  public let CONST = MigrationTypes.Current.CONST;

  public let Set = MigrationTypes.Current.Set;

  public let Map = MigrationTypes.Current.Map;
  public let {phash; thash; nhash} = Map;
  public let Vector = MigrationTypes.Current.Vector;
  public let BTree = MigrationTypes.Current.BTree;
  public let governance = MigrationTypes.Current.governance;


  public let ONE_MINUTE = 60000000000 : Nat; //NanoSeconds

  public func initialState() : State {#v0_0_0(#data)};
  public let currentStateVersion = #v0_1_0(#id);

  type ConfigMap  = Map.Map<Text, ICRC16>;

  public class Orchestrator(stored: ?State, canister: Principal, environment: Environment){

    var state : CurrentState = switch(stored){
        case(null) {
          let #v0_1_0(#data(foundState)) = init(initialState(),currentStateVersion, null, canister);
          foundState;
        };
        case(?val) {
          let #v0_1_0(#data(foundState)) = init(val, currentStateVersion, null, canister);
          foundState;
        };
      };

    

    private func natNow(): Nat{Int.abs(Time.now())};

    public func isApprovedPublisher(caller: Principal, record: PublicationRecord) : async* Star.Star<Bool, GenericError>{
      switch(record.allowedPublishers){
          case (?#allowed(allowedPublishers)) {
            let ?foundCaller = Set.contains(allowedPublishers, phash, caller) else return #trappable(false);
            #trappable(true);
          };
          case (?#disallowed(disallowedPublishers)) {
            let ?foundCaller = Set.contains(disallowedPublishers, phash, caller) else return #trappable(true);
            #trappable(false);
          };
          case(?#allowed_icrc75(allowedSubscribers)) {
            let icrc75Service : ICRC75Service.Service = actor(Principal.toText(allowedSubscribers.principal));
            let result = try{
              await icrc75Service.icrc75_is_member([(#Identity(caller), [[allowedSubscribers.namespace]])]);
            } catch (e){
              return #err(#awaited({error_code = 0; message = "ICRC75 error:" # Error.message(e)}));
            };
            return #awaited(result[0]);
          };
          case(?#disallowed_icrc75(allowedSubscribers)) {
            let icrc75Service : ICRC75Service.Service = actor(Principal.toText(allowedSubscribers.principal));
            let result = try{
              await icrc75Service.icrc75_is_member([(#Identity(caller), [[allowedSubscribers.namespace]])]);
            }  catch (e){
              return #err(#awaited({error_code = 0; message = "ICRC75 error:" # Error.message(e)}));
            };

            return #awaited(not result[0]);
          };
          case(null) return #trappable(true);
        };
    };

    public func isApprovedSubscriber(caller: Principal, record: PublicationRecord) : async* Star.Star<Bool, GenericError>{
      switch(record.allowedSubscribers){
          case (?#allowed(allowedSubscribers)) {
            let ?foundCaller = Set.contains(allowedSubscribers, phash, caller) else return #trappable(false);
            #trappable(true);
          };
          case (?#disallowed(disallowedSubscribers)) {
            let ?foundCaller = Set.contains(disallowedSubscribers, phash, caller) else return #trappable(true);
            #trappable(false);
          };
          case(?#allowed_icrc75(allowedSubscribers)) {
            let icrc75Service : ICRC75Service.Service = actor(Principal.toText(allowedSubscribers.principal));
            let result = try{
              await icrc75Service.icrc75_is_member([(#Identity(caller), [[allowedSubscribers.namespace]])]);
            } catch (e){
              return #err(#awaited({error_code = 0; message = "ICRC75 error:" # Error.message(e)}));
            };
            return #awaited(result[0]);
          };
          case(?#disallowed_icrc75(allowedSubscribers)) {
            let icrc75Service : ICRC75Service.Service = actor(Principal.toText(allowedSubscribers.principal));
            let result = try{
              await icrc75Service.icrc75_is_member([(#Identity(caller), [[allowedSubscribers.namespace]])]);
            }  catch (e){
              return #err(#awaited({error_code = 0; message = "ICRC75 error:" # Error.message(e)}));
            };

            return #awaited(not result[0]);
          };
          case(null) return #trappable(true);
        };
    };

    private func starError(awaited: Bool, number: Nat, message: Text) : Star.Star<TT.ActionId, TT.Error> {
      if(awaited){
        return #err(#awaited({error_code = number; message = message}));
      } else {
        return #err(#trappable({error_code = number; message = message}));
      }
    };

    ///MARK: Asgn BC to Pub
    private func canAssignBroadcaster(actionId: TT.ActionId, action: TT.Action) : async* Star.Star<TT.ActionId, TT.Error> {

      //todo: if there is an error, we need to broadcast to the requesting publisher that the broadcaster could not be assigned
   
      
      if(action.actionType== CONST.publication.actions.canAssignBroadcaster){

          var bAwaited = false;
          let ?params = from_candid(action.params) : ?(Nat, Principal) else return starError(bAwaited, 1, "candid error");
          let existing = switch(BTree.get(state.publications, Nat.compare, params.0)){
            case (?existing) existing;
            case(null){
              return #err(#trappable({error_code = 1001; message = "publication not found"}));
            };
          };
        
          let approved = switch(await* isApprovedPublisher(params.1, existing)){
            case(#trappable(val)) val;
            case(#awaited(val)) {
              bAwaited := true;
              val;
            };
            case(#err(#trappable(err))) return #err(#trappable(err));
            case(#err(#awaited(err))) return #err(#awaited(err));
          };
          if(approved){
            
            D.print("candidParsed" # debug_show(params));
            let existingPublisher = Map.get<Principal, PublisherRecord>(existing.registeredPublishers, Map.phash, params.1);
            
            let foundPublisher = switch(existingPublisher){
              case (?publisherRecord) publisherRecord;
              case (null) {
                //need to create the publisher record

                //get subnet
                let #Ok(subnet) = try{await governance.get_subnet_for_canister({principal = ?params.1})} catch(e){
                  return starError(bAwaited, 2, "subnet error");
                } else return starError(bAwaited, 2, "subnet error");

                let ?foundSubnet = subnet.subnet_id else return starError(bAwaited, 2, "empty subnet error");

                //do we have a broadcaster on this subnet?
                let foundBroadcaster = switch(Map.get(state.broadcastersBySubnet, phash, foundSubnet)){
                  case(?broadcaster) broadcaster;
                  case (null) {
                    //can we deploy one?
                    //todo: Maybe provide a way to deply a broadcaster dynamically
                    return starError(bAwaited, 3, "subnet not supported");
                  };
                };

                let newPublisherRecord : PublisherRecord = {
                  broadcasters = Set.fromIter<Principal>([foundBroadcaster].vals(), phash);
                  subnet = foundSubnet;
                };

                ignore Map.put(existing.registeredPublishers, phash, params.1, newPublisherRecord);

                //todo: 72BroadcasterAssign

                newPublisherRecord;

              };
            };

            //todo: emit the event to the publisher that it has an assigned broadcaster

            if(bAwaited){
              return #awaited(actionId);
            } else {
              return #trappable(actionId);
            };

          } else {
            return #err(#trappable({error_code = 0; message = "Publisher not approved"}));
          };
        } else {
          #err(#trappable({error_code = 0; message = "Unknown action type"}));
        };
      
    };

    ///MARK: Asgn BC to Sub
    private func assignBroadcasterToSubscriber(actionId: TT.ActionId, action: TT.Action) : async* Star.Star<TT.ActionId, TT.Error> {

      //todo: if there is an error, we need to broadcast to the requesting subscriber  that the broadcaster could not be assigned

      let foundStake = 0; //todo: figure out staking
   
      
      if(action.actionType== CONST.publication.actions.assignBroadcasterToSubscriber){

          var bAwaited = false;

          let ?params = from_candid(action.params) : ?(Nat, Principal, ICRC16Map) else return starError(bAwaited, 1, "candid error");

          let existingSubscription = switch(BTree.get(state.subscriptions, Nat.compare, params.0)){
            case (?existing) existing;
            case(null){
              return #err(#trappable({error_code = 1001; message = "subscription not found"}));
            };
          };

          let existingPublication = switch(BTree.get(state.publications, Nat.compare, existingSubscription.publicationId)){
            case (?existing) existing;
            case(null){
              return #err(#trappable({error_code = 1001; message = "publication not found"}));
            };
          };

          let approved = switch(await* isApprovedSubscriber(params.1, existingPublication)){
            case(#trappable(val)) val;
            case(#awaited(val)) {
              bAwaited := true;
              val;
            };
            case(#err(#trappable(err))) return #err(#trappable(err));
            case(#err(#awaited(err))) return #err(#awaited(err));
          };

          if(approved){

            let configMap = Map.fromIter<Text, ICRC16>(params.2.vals(), thash);

            let existingSubscriber = switch(Map.get<Principal, SubscriberRecord>(existingSubscription.subscribers, Map.phash, params.1)){
              case (?subscriberRecord) subscriberRecord;
              case (null) {
                //need to create the subscriber record
                let newSubscriber  : SubscriberRecord = {
                  subscriptionId = existingSubscription.id;
                  initialConfig = existingSubscription.initialConfig;
                  var stake = 0;
                  var filter = getSubFilter(configMap);
                  var bActive = getSubActive(configMap);
                  var skip = getSubSkip(configMap);
                  subscriber = params.1;
                  var subnet = null;
                  registeredBroadcasters = Set.new<Principal>();
                };
                ignore Map.put(existingSubscription.subscribers, phash, params.1, newSubscriber);
                newSubscriber;
                
              };
            };

            //get subnet
            let subscriberSubnet = switch(existingSubscriber.subnet){
              case(null){
                let #Ok(subnet) = try{await governance.get_subnet_for_canister({principal = ?params.1})} catch(e){
                    return starError(bAwaited, 2, "subnet error");
                  } else return starError(bAwaited, 2, "subnet error");

                  let ?foundSubnet = subnet.subnet_id else return starError(bAwaited, 2, "empty subnet error");
                  existingSubscriber.subnet := ?foundSubnet;
                  foundSubnet;
              };
              case(?subnet) subnet;
            };

            let thisSubnetBroadcaster : Principal = switch(Map.get(state.broadcastersBySubnet, phash, subscriberSubnet)){
              case(?broadcaster) broadcaster;
              case (null) {
                //todo: can we deploy one?
                //todo; emit errors
                return starError(bAwaited, 3, "subnet not supported");
              };
            };

            //For every publisher that is not on the same subnet we need to add a relay to that publisher's broadcaster's
            for(publisher in Map.entries(existingPublication.registeredPublishers)){
              if(publisher.1.subnet != subscriberSubnet){
                //publishers that don't exist on this subnet need to be relayed to
                //add this subnet to the relay if it doesn't exist
                for(thisBroadcaster in Set.keys(publisher.1.broadcasters)){
                  let foundBroadcaster = switch(BTree.get(state.broadcasters, Principal.compare, thisBroadcaster)){
                    
                    case(?broadcaster) broadcaster;
                    case (null) {
                      //can we deploy one? should be unreachable
                      //todo; emit errors
                      return starError(bAwaited, 3, "broadcaster not found");
                    };
                  };

                  let foundRelay = switch(Map.get(foundBroadcaster.relays, thash, existingPublication.namespace)){
                    case(?relay) relay;
                    case (null) {
                      let newRelayRecord = (Map.new<Principal, Set.Set<Principal>>(), BTree.init<Nat, (Principal,Principal)>(null));
                      ignore Map.put(foundBroadcaster.relays, Map.thash, existingPublication.namespace, newRelayRecord);
                      newRelayRecord;
                    };
                  };

                  //todo: 72RelayAssign
                  let foundRelaySet = switch(Map.get(foundRelay.0, phash, thisSubnetBroadcaster)){
                    case(?relaySet) relaySet;
                    case (null) {
                      let newRelaySet = Set.new<Principal>();
                      ignore Map.put(foundRelay.0, Map.phash, thisSubnetBroadcaster, newRelaySet);
                      newRelaySet;
                    };
                  };

                  //todo: 72SubscritionAssign

                  //add to the Main Dictionary
                  Set.add(foundRelaySet, phash, params.1);

                  //add to the stake index
                  ignore BTree.insert(foundRelay.1, Nat.compare, foundStake, (thisSubnetBroadcaster, params.1));

                  //add to the registered broadcasters
                  Set.add(existingSubscriber.registeredBroadcasters, phash, thisSubnetBroadcaster);
                };
              } else {
                //add this subscriber to the broadcaster's list
                let foundBroadcaster = switch(BTree.get(state.broadcasters, Principal.compare, thisSubnetBroadcaster)){
                  case(?broadcaster) broadcaster;
                  case (null) {
                    //can we deploy one? should be unreachable
                    //todo; emit errors
                    return starError(bAwaited, 3, "broadcaster not found");
                  };
                };

                let foundNamespace = switch(Map.get(foundBroadcaster.subscribers, thash, existingPublication.namespace)){
                  case(?namespace) namespace;
                  case (null) {
                    let newNamespaceRecord = (Set.new<Principal>(), BTree.init<Nat, Principal>(null));
                    ignore Map.put(foundBroadcaster.subscribers, Map.thash, existingPublication.namespace, newNamespaceRecord );
                    newNamespaceRecord;
                  };
                };

                //todo: 72SubscritionAssign
                Set.add(foundNamespace.0, phash, params.1);
                ignore BTree.insert(foundNamespace.1, Nat.compare, existingSubscriber.stake, params.1);
                Set.add(existingSubscriber.registeredBroadcasters, phash, params.1);
              };
            };

            //todo: emit the event to the subscriber that it has an assigned broadcaster
            //todo: emit the event to the broadcaster that it has an assigned relay or subscriber

            if(bAwaited){
              return #awaited(actionId);
            } else {
              return #trappable(actionId);
            };

          } else {
            return #err(#trappable({error_code = 0; message = "Publisher not approved"}));
          };
        } else {
          #err(#trappable({error_code = 0; message = "Unknown action type"}));
        };
    };

    

    public func icrc72_register_publication(caller: Principal, request:[PublicationRegistration]) : async* [PublicationRegisterResult]{
      //todo: security check

      return await* register_publication(caller, request, null);
    };

    private func getPrincipalListFromMap(configMap: ConfigMap, key: Text) : ?Set.Set<Principal>{
      let ?#Array(items) = Map.get(configMap, thash, key) else return null;
      let list = Set.new<Principal>();
      for(item in items.vals()){
        let #Blob(principal) = item else return null;
        Set.add(list, phash, Principal.fromBlob(principal));
      };
      return ?list;
    };

    private func getAllowedPublishers(configMap: ConfigMap) : ?Set.Set<Principal>{
      return getPrincipalListFromMap(configMap, CONST.publication.publishers.allowed.list);
    };

    private func getdisallowedPublishers(configMap: ConfigMap) : ?Set.Set<Principal>{
      return getPrincipalListFromMap(configMap, CONST.publication.publishers.disallowed.list);
    };

    private func getAllowedSubscribers(configMap: ConfigMap) : ?Set.Set<Principal>{
      return getPrincipalListFromMap(configMap, CONST.publication.subscribers.allowed.list);
    };

    private func getdisallowedSubscribers(configMap: ConfigMap) : ?Set.Set<Principal>{
      return getPrincipalListFromMap(configMap, CONST.publication.subscribers.disallowed.list);
    };

    private func getICRC75Permission(configMap: ConfigMap, key: Text) : ?ICRC75Item{
      let ?item = Map.get(configMap, thash, key) else return null;
      let #Array(items) = item else return null;
      if(items.size() != 2){
        return null;
      };
      let #Blob(principal) = items[0] else return null;
      let #Text(namespace) = items[1] else return null;
      return ?{principal = Principal.fromBlob(principal);
        namespace = namespace};
    };

    private func getPublicationControllers(caller: Principal, configMap: ConfigMap) : Set.Set<Principal> {
      switch(getPrincipalListFromMap(configMap, CONST.publication.controllers.list)){
        case(?val) val;
        case(null) Set.fromIter<Principal>([caller].vals(),phash);
      };
    };

    private func getSubscriptionControllers(caller: Principal, configMap: ConfigMap) : Set.Set<Principal> {
      switch(getPrincipalListFromMap(configMap, CONST.subscription.controllers.list)){
        case(?val) val;
        case(null) Set.fromIter<Principal>([].vals(), phash);
      };
    };

    private func getICRC75AllowedPublishers(configMap: ConfigMap) : ?ICRC75Item{
      return getICRC75Permission(configMap, CONST.publication.publishers.allowed.icrc75);
    };

    private func getICRC75DisallowedPublishers(configMap: ConfigMap) : ?ICRC75Item{
      return getICRC75Permission(configMap, CONST.publication.publishers.disallowed.icrc75);
    };

    private func getICRC75AllowedSubscribers(configMap: ConfigMap) : ?ICRC75Item{
      return getICRC75Permission(configMap, CONST.publication.subscribers.disallowed.icrc75);
    };

    private func getICRC75DisallowedSubscribers(configMap: ConfigMap) : ?ICRC75Item{
      return getICRC75Permission(configMap, CONST.publication.subscribers.disallowed.icrc75);
    };

    private func getPublicationPublisherPermission(configMap: ConfigMap) : ?PermissionSet {
      switch(getAllowedPublishers(configMap)){
        case (?allowedPublishers) ?#allowed(allowedPublishers);
        case (null) {
          switch(getdisallowedPublishers(configMap)){
            case (?disallowedPublishers) ?#disallowed(disallowedPublishers);
            case (null) {
              switch(getICRC75AllowedPublishers(configMap)){
                case (?allowedSubscribers) ?#allowed_icrc75(allowedSubscribers);
                case (null) {
                  switch(getICRC75DisallowedPublishers(configMap)){
                    case (?disallowedSubscribers) ?#disallowed_icrc75(disallowedSubscribers);
                    case (null) null;
                  };
                };
              };
            };
          };
        };
      };
    };

    private func getPublicationSubscriberPermission(configMap: ConfigMap) : ?PermissionSet {
      switch(getAllowedSubscribers(configMap)){
        case (?allowedSubscribers) ?#allowed(allowedSubscribers);
        case (null) {
          switch(getdisallowedSubscribers(configMap)){
            case (?disallowedSubscribers) ?#disallowed(disallowedSubscribers);
            case (null) {
              switch(getICRC75AllowedSubscribers(configMap)){
                case (?allowedSubscribers) ?#allowed_icrc75(allowedSubscribers);
                case (null) {
                  switch(getICRC75DisallowedSubscribers(configMap)){
                    case (?disallowedSubscribers) ?#disallowed_icrc75(disallowedSubscribers);
                    case (null) null;
                  };
                };
              };
            };
          };
        };
      };
    };


    public func register_publication<system>(caller: Principal, request:[PublicationRegistration], canAddPublication: CanAddPublication) : async* [PublicationRegisterResult]{

      let results = Vector.Vector<PublicationRegisterResult>();

      label proc for(thisItem : PublicationRegistration  in request.vals()){
        
        //see if we already have the publication
        let existing = switch(BTree.get(state.publicationsByNamespace, Text.compare, thisItem.namespace)){
          case(null) null;
          case(?val) switch(BTree.get(state.publications, Nat.compare, val)){
            case(null) null;
            case(?existing) ?existing;
          };
        };

        switch(existing){
          case (?existing) {
        
            //todo: do we want to make sure the type matches? someone could add a bunch of timers here...we will need to add a cool down
            //this publicatin exists, but a new publisher is registering it so we may need to assign a brodcaster and let them know...but we need to know they have permissions, so we'll set that up via the timer tool.
            ignore environment.tt.setActionASync<system>(natNow(), {actionType = CONST.publication.actions.canAssignBroadcaster; params= to_candid((existing.id, caller))}, ONE_MINUTE * 5);
            results.add(?#Ok(existing.id));

            continue proc;
          
          };
          case (null) {};
        };

        let parsedItem : PublicationRegistration = switch(canAddPublication){
          case (?interceptor) {
            switch(await* interceptor(caller, thisItem)){
              case (#awaited(item)) item;
              case (#trappable(item)) item;
              case (#err(#trappable(err))){
                results.add(?#Err(#GenericError({
                  error_code = 0;
                  message = "trappable: " #debug_show(err);
                })));
                continue proc;
              };
              case (#err(#awaited(err))){
                 results.add(?#Err(#GenericError({
                  error_code = 0;
                  message = "awaited: " # debug_show(err);
                })));
                continue proc;
              };
            };
          };
          case (null) thisItem;
        };

        let configMap = Map.fromIter<Text, ICRC16>(parsedItem.config.vals(), thash);

        //do we need to gate this somehow? Likely yes : TODO: gate canAddPublication
        let newPublication ={
          id = state.nextPublicationID;
          namespace = parsedItem.namespace; // The namespace of the publication
          initialConfig = parsedItem.config; // Additional configuration or metadata about the publication
          var allowedPublishers = getPublicationPublisherPermission(configMap); // List of publishers allowed to publish under this namespace
          var allowedSubscribers = getPublicationSubscriberPermission(configMap); // List of subscribers allowed to subscribe to this namespace
          registeredPublishers = Map.new<Principal, PublisherRecord>(); // Map of publishers registered and their assigned broadcasters
          //todo: These may be handled under a different ICRC eventually...we may have to handle staitiscs a certain way
          subnetIndex = Map.new<Principal, Principal>(); // Map of publishers registered and their assigned broadcasters
          var eventCount = 0;
          var eventsSent  = 0;
          var notifications  = 0;
          var confrimations  = 0;
          controllers = getPublicationControllers(caller, configMap); // List of controllers for the publication
          } : PublicationRecord;

        ignore BTree.insert(state.publications, Nat.compare, newPublication.id, newPublication);
        ignore BTree.insert(state.publicationsByNamespace, Text.compare, newPublication.namespace, newPublication.id);

        results.add(?#Ok(state.nextPublicationID));

        state.nextPublicationID += 1;

        let trxid = switch(environment.add_record){
          case (?addRecord) {
            //todo: calculate value of blocks
            let txtop = Vector.fromIter<(Text, Value)>([("btype",#Text("72PubReg")),("ts", #Nat(natNow()))].vals());
            let tx = Vector.fromIter<(Text, Value)>([
              ("namespace", #Text(newPublication.namespace) : Value) : (Text,Value),
              ("config", Conversion.CandySharedToValue(#Map(parsedItem.config) : Candy.CandyShared): Value): (Text,Value),
              ("publicationId", #Nat(newPublication.id): Value ) : (Text,Value),
            ].vals());
            switch(thisItem.memo){
              case (?memo) {
                tx.add(("memo", #Blob(memo)));
              };
              case (null) {};
            };
            addRecord(Vector.toArray(tx), ?Vector.toArray(txtop));
          };
          case (null) 0;
        };

        //emit to any listeners
        for(listener in publicationRegisteredListeners.vals()){
          listener.1<system>(newPublication, trxid);
        };

        ignore environment.tt.setActionASync<system>(natNow(), {actionType = CONST.publication.actions.canAssignBroadcaster; params= to_candid((newPublication.namespace, caller))}, ONE_MINUTE * 5);
      };



      return Vector.toArray(results);
    };

    public func icrc72_update_publication(caller: Principal, request:[PublicationUpdateRequest]) : async* [PublicationUpdateResult]{
      //todo: security check

      return await* update_publication(caller, request, null);
    };

    public func update_publication<system>(caller: Principal, request:[PublicationUpdateRequest], canUpdatePublication: CanUpdatePublication) : async* [PublicationUpdateResult]{

      let results = Vector.Vector<PublicationUpdateResult>();

      label proc for(thisItem : PublicationUpdateRequest  in request.vals()){
        
        //see if we already have the publication
        let existing = switch(thisItem.publication){
          case(#namespace(val)){
            switch(BTree.get(state.publicationsByNamespace, Text.compare, val)){
              case(null) null;
              case(?existing) switch(BTree.get(state.publications, Nat.compare,existing)){
                case(null) null;
                case(?existing) ?existing;
              };
            };
          };
          case(#publicationId(val)){
            switch(BTree.get(state.publications, Nat.compare, val)){
              case(null) null;
              case(?existing) ?existing;
            };
          };
        };

        
        switch(existing){
          case (?existing) {
            //make sure we are a controller
            switch(Set.contains<Principal>(existing.controllers, Set.phash, caller)){
              case(?val){

                let parsedItem : PublicationUpdateRequest = switch(canUpdatePublication){
                  case (?interceptor) {
                    switch(await* interceptor(caller, thisItem)){
                      case (#awaited(item)) item;
                      case (#trappable(item)) item;
                      case (#err(#trappable(err))){
                        results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "trappable: " #debug_show(err);
                        })));
                        continue proc;
                      };
                      case (#err(#awaited(err))){
                        results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "awaited: " # debug_show(err);
                        })));
                        continue proc;
                      };
                    };
                  };
                  case (null) thisItem;
                };
                
                //we need to update the settings, looking for a flag to see if we need to alert a publisher

              
                if(parsedItem.config.0 == CONST.publication.publishers.allowed.list_add){
                  ///MARK: Add Alw Pub
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          //need to revert here...hmmm
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedPublishers){
                    case (?#allowed(allowedPublishers)) {
                      for(thisItem in items){
                        Set.add(allowedPublishers, phash, thisItem);
                      };
                    };
                    case (_) {
                      existing.allowedPublishers := ?#allowed(Set.fromIter<Principal>(items, phash));
                    };
                  };

                } else if (parsedItem.config.0 == CONST.publication.publishers.allowed.list_remove){
                  ///MARK: Rem Alw Pub
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedPublishers){
                    case (?#allowed(allowedPublishers)) {
                      for(thisItem in items){
                        ignore Set.remove(allowedPublishers, phash, thisItem);
                      };
                    };
                    case (_) {
                      results.add(?#Err(#GenericError({
                        error_code = 0;
                        message = "Invalid operation: not a set to allowed list";
                      })));
                    };
                  };
                } else if(parsedItem.config.0 == CONST.publication.publishers.disallowed.list_add){
                  ///MARK: Add DAlw Pub
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          //need to revert here...hmmm
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedPublishers){
                    case (?#disallowed(allowedPublishers)) {
                      for(thisItem in items){
                        Set.add(allowedPublishers, phash, thisItem);
                      };
                    };
                    case (_) {
                      existing.allowedPublishers := ?#disallowed(Set.fromIter<Principal>(items, phash));
                    };
                  };

                } else if (parsedItem.config.0 == CONST.publication.publishers.allowed.list_remove){
                  ///MARK: Rem DAlw Pub
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedPublishers){
                    case (?#disallowed(allowedPublishers)) {
                      for(thisItem in items){
                        ignore Set.remove(allowedPublishers, phash, thisItem);
                      };
                    };
                    case (_) {
                      results.add(?#Err(#GenericError({
                        error_code = 0;
                        message = "Invalid operation: not a set to disallowed list";
                      })));
                    };
                  };
                } else if(parsedItem.config.0 == CONST.publication.publishers.allowed.icrc75_update){
                  ///MARK: Add Alw 75
                  let config = switch(getICRC75Permission(Map.fromIter<Text,ICRC16>([parsedItem.config].vals(), thash), CONST.publication.publishers.allowed.icrc75_update)){
                    case(?config)config;
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid icrc75";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedPublishers){
                    case(?#allowed_icrc75(_)){
                      existing.allowedPublishers := ?#allowed_icrc75(config);
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid operation: not an icrc75";
                        })));
                        continue proc;
                    };
                  };
                } else if (parsedItem.config.0 == CONST.publication.publishers.allowed.icrc75_remove){
                  ///MARK: Rem Alw 75
                  
                  switch(existing.allowedPublishers){
                    case (?#allowed_icrc75(allowedPublishers)) {
                      existing.allowedPublishers := null;
                    };
                    case (_) {
                      results.add(?#Err(#GenericError({
                        error_code = 0;
                        message = "Invalid operation: not a icrc75 allow list";
                      })));
                    };
                  };
                } else if(parsedItem.config.0 == CONST.publication.publishers.disallowed.icrc75_update){
                  ///MARK: Add DAlw 75
                  let config = switch(getICRC75Permission(Map.fromIter<Text,ICRC16>([parsedItem.config].vals(), thash), CONST.publication.publishers.allowed.icrc75_update)){
                    case(?config)config;
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid icrc75";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedPublishers){
                    case(?#disallowed_icrc75(_)){
                      existing.allowedPublishers := ?#disallowed_icrc75(config);
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid operation: not an icrc75";
                        })));
                        continue proc;
                    };
                  };
                } else if (parsedItem.config.0 == CONST.publication.publishers.disallowed.icrc75_remove){
                  ///MARK: Rem DAlw 75
                  
                  switch(existing.allowedPublishers){
                    case (?#disallowed_icrc75(allowedPublishers)) {
                      existing.allowedPublishers := null;
                    };
                    case (_) {
                      results.add(?#Err(#GenericError({
                        error_code = 0;
                        message = "Invalid operation: not a icrc75 allow list";
                      })));
                    };
                  };
                } else if(parsedItem.config.0 == CONST.publication.subscribers.allowed.list_add){
                  ///MARK: Add Alw Sub
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          //need to revert here...hmmm
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedSubscribers){
                    case (?#allowed(allowedSubscribers)) {
                      for(thisItem in items){
                        Set.add(allowedSubscribers, phash, thisItem);
                      };
                    };
                    case (_) {
                      existing.allowedSubscribers := ?#allowed(Set.fromIter<Principal>(items, phash));
                    };
                  };

                } else if (parsedItem.config.0 == CONST.publication.subscribers.allowed.list_remove){
                  ///MARK: Rem Alw Sub
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedSubscribers){
                    case (?#allowed(allowedSubscribers)) {
                      for(thisItem in items){
                        ignore Set.remove(allowedSubscribers, phash, thisItem);
                      };
                    };
                    case (_) {
                      results.add(?#Err(#GenericError({
                        error_code = 0;
                        message = "Invalid operation: not a set to allowed list";
                      })));
                    };
                  };
                } else if(parsedItem.config.0 == CONST.publication.subscribers.disallowed.list_add){
                  ///MARK: Add DAlw Sub
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          //need to revert here...hmmm
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedSubscribers){
                    case (?#disallowed(allowedSubscribers)) {
                      for(thisItem in items){
                        Set.add(allowedSubscribers, phash, thisItem);
                      };
                    };
                    case (_) {
                      existing.allowedSubscribers := ?#disallowed(Set.fromIter<Principal>(items, phash));
                    };
                  };

                } else if (parsedItem.config.0 == CONST.publication.subscribers.allowed.list_remove){
                  ///MARK: Rem DAlw Sub
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedSubscribers){
                    case (?#disallowed(allowedSubscribers)) {
                      for(thisItem in items){
                        ignore Set.remove(allowedSubscribers, phash, thisItem);
                      };
                    };
                    case (_) {
                      results.add(?#Err(#GenericError({
                        error_code = 0;
                        message = "Invalid operation: not a set to disallowed list";
                      })));
                    };
                  };
                } else if(parsedItem.config.0 == CONST.publication.subscribers.allowed.icrc75_update){
                  ///MARK: Add Alw 75 S
                  let config = switch(getICRC75Permission(Map.fromIter<Text,ICRC16>([parsedItem.config].vals(), thash), CONST.publication.subscribers.allowed.icrc75_update)){
                    case(?config)config;
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid icrc75";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedSubscribers){
                    case(?#allowed_icrc75(_)){
                      existing.allowedSubscribers := ?#allowed_icrc75(config);
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid operation: not an icrc75";
                        })));
                        continue proc;
                    };
                  };
                } else if (parsedItem.config.0 == CONST.publication.subscribers.allowed.icrc75_remove){
                  ///MARK: Rem Alw 75 S
                  
                  switch(existing.allowedSubscribers){
                    case (?#allowed_icrc75(allowedSubscribers)) {
                      existing.allowedSubscribers := null;
                    };
                    case (_) {
                      results.add(?#Err(#GenericError({
                        error_code = 0;
                        message = "Invalid operation: not a icrc75 allow list";
                      })));
                    };
                  };
                } else if(parsedItem.config.0 == CONST.publication.subscribers.disallowed.icrc75_update){
                  ///MARK: Add DAlw 75 S
                  let config = switch(getICRC75Permission(Map.fromIter<Text,ICRC16>([parsedItem.config].vals(), thash), CONST.publication.subscribers.allowed.icrc75_update)){
                    case(?config)config;
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid icrc75";
                        })));
                        continue proc;
                    };
                  };
                  switch(existing.allowedSubscribers){
                    case(?#disallowed_icrc75(_)){
                      existing.allowedSubscribers := ?#disallowed_icrc75(config);
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid operation: not an icrc75";
                        })));
                        continue proc;
                    };
                  };
                } else if (parsedItem.config.0 == CONST.publication.subscribers.disallowed.icrc75_remove){
                  ///MARK: Rem DAlw 75 s
                  
                  switch(existing.allowedSubscribers){
                    case (?#disallowed_icrc75(allowedSubscribers)) {
                      existing.allowedSubscribers := null;
                    };
                    case (_) {
                      results.add(?#Err(#GenericError({
                        error_code = 0;
                        message = "Invalid operation: not a icrc75 allow list";
                      })));
                    };
                  };
                } else if(parsedItem.config.0 == CONST.publication.controllers.list_add){
                  ///MARK: Add CTRl
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          //need to revert here...hmmm
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                  
                  for(thisItem in items){
                    Set.add(existing.controllers, phash, thisItem);
                  };
                    

                } else if (parsedItem.config.0 == CONST.publication.controllers.list_remove){
                  ///MARK: Rem CTRl
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                 
                  for(thisItem in items){
                    ignore Set.remove(existing.controllers, phash, thisItem);
                  };
                    
                
                } ;

                let trxid = switch(environment.add_record){
                  case (?addRecord) {
                    //todo: calculate value of blocks
                    let txtop = Vector.fromIter<(Text, Value)>([("btype",#Text("72PubReg")),("ts", #Nat(Int.abs(Time.now())))].vals());
                    let tx = Vector.fromIter<(Text, Value)>([
                      ("namespace", #Text(existing.namespace) : Value) : (Text,Value),
                      ("config", Conversion.CandySharedToValue(#Map([parsedItem.config]) : Candy.CandyShared): Value): (Text,Value),
                      ("publicationId", #Nat(existing.id): Value ) : (Text,Value),
                    ].vals());
                    switch(thisItem.memo){
                      case (?memo) {
                        tx.add(("memo", #Blob(memo)));
                      };
                      case (null) {};
                    };
                    addRecord(Vector.toArray(tx), ?Vector.toArray(txtop));
                  };
                  case (null) 0;
                };

                //emit to any listeners
                for(listener in publicationRegisteredListeners.vals()){
                  listener.1<system>(existing, trxid);
                };
              };
              case(null) {
                results.add(?#Err(#Unauthorized));
                continue proc;
              };
            

            
            };
          };
          case (null) {
            results.add(?#Err(#NotFound));
            continue proc;
          };
        };
      };

      return Vector.toArray(results);
    };

    public func icrc72_register_subscription(caller: Principal, request:[SubscriptionRegistration]) : async* [SubscriptionRegisterResult]{
      return await* register_subscription(caller, request, null);
    };

    private func fileSubscription(subscription: SubscriptionRecord) : () {
       
      let rec = switch(BTree.get(state.subscriptions, Nat.compare, subscription.id)){
        case (?existing) existing;
        case (null) {
          ignore BTree.insert(state.subscriptions, Nat.compare, subscription.id, subscription);
          subscription;
        };
      };

      ignore BTree.insert(state.subscriptionsByNamespace , Text.compare, rec.namespace, rec.id);
    };

    private func fileSubscriber(subscription: SubscriptionRecord, subscriber: SubscriberRecord) : () {


      let idx = switch(BTree.get(state.subscribersByPrincipal, Principal.compare, subscriber.subscriber)){
        case (?existing) existing;
        case (null) {
          let newCol = Set.new<Nat>();
          ignore BTree.insert(state.subscribersByPrincipal, Principal.compare, subscriber.subscriber, newCol);
          newCol;
        };
      };

      ignore Set.add(idx, nhash, subscription.id);

      ignore Map.put(subscription.subscribers, phash, subscriber.subscriber, subscriber);


      ignore BTree.insert(subscription.stakeIndex, Nat.compare, subscriber.stake, subscriber.subscriber);
    };

    private func getSubFilter(configMap: ConfigMap) : ?Text{
      let ?#Text(filter) = Map.get(configMap, thash, CONST.subscription.filter) else return null;
      ?filter
    };

    private func getSubActive(configMap: ConfigMap) : Bool{
      let ?#Text(active) = Map.get(configMap, thash, CONST.subscription.filter) else return true;
      if(active == "false"){
        false;
      } else {
        true;
      };
    };

    private func getSubSkip(configMap: ConfigMap) : ?(Nat, Nat){
      let ?#Array(details) = Map.get(configMap, thash, CONST.subscription.skip) else return null;
      if(details.size() == 1){
        let #Nat(skip) = details[0] else return null;
        return ?(skip, 0);
      };
      if(details.size() == 2){
        let #Nat(skip) = details[0] else return null;
        let #Nat(offset) = details[1] else return null;
        return ?(skip, offset);
      };
      return null;
    };

    private func getSubscriberFromUpdate(subscriptionUpdate: SubscriptionUpdateRequest, subscription: SubscriptionRecord) : ?SubscriberRecord{
      switch(subscriptionUpdate.subscriber){
        case(null) null;
        case(?val){
          Map.get(subscription.subscribers, phash, val);
        };
      };
    };

    

    public func register_subscription(caller: Principal, request:[SubscriptionRegistration], canAddSubscription: CanAddSubscription) : async* [SubscriptionRegisterResult]{
        
        let results = Vector.Vector<SubscriptionRegisterResult>();

        var bAwaited = false;
  
        label proc for(thisItem : SubscriptionRegistration  in request.vals()){

          let ?publication = switch(BTree.get(state.publicationsByNamespace, Text.compare, thisItem.namespace)){
            case(null) null;
            case(?val) switch(BTree.get(state.publications, Nat.compare, val)){
              case(null) null;
              case(?existing) ?existing;
            };
          } else {
            results.add(?#Err(#PublicationNotFound));
            continue proc;
          };

          //todo: This can be an async call whic will really slow things down...we need to figure out how to handle this with Tokens and a permission in the title or a cached item.
          let approval = switch(await* isApprovedSubscriber(caller, publication)){
            case(#trappable(val)) val;
            case(#awaited(val)) val;
            case(#err(#trappable(err))){
              results.add(?#Err(#Unauthorized));
              continue proc;
            };
            case(#err(#awaited(err))){
              results.add(?#Err(#Unauthorized));
              continue proc;
            };
          };

          if(approval == false){
            results.add(?#Err(#Unauthorized));
            continue proc;
          };

          let parsedItem : SubscriptionRegistration = switch(canAddSubscription){
            case (?interceptor) {
              switch(await* interceptor(caller, thisItem)){
                case (#awaited(item)) item;
                case (#trappable(item)) item;
                case (#err(#trappable(err))){
                  results.add(?#Err(#GenericError({
                    error_code = 0;
                    message = "trappable: " #debug_show(err);
                  })));
                  continue proc;
                };
                case (#err(#awaited(err))){
                  results.add(?#Err(#GenericError({
                    error_code = 0;
                    message = "awaited: " # debug_show(err);
                  })));
                  continue proc;
                };
              };
            };
            case (null) thisItem;
          };

          let configMap = let configMap = Map.fromIter<Text, ICRC16>(parsedItem.config.vals(), thash);
          
          //see if we already have the subscription
          let existingID = switch(BTree.get(state.subscriptionsByNamespace, Text.compare, thisItem.namespace)){
            case(?existing) ?existing ;
            case(null) null;
          };

          let existing = switch(existingID){
            case(?existing) switch(BTree.get(state.subscriptions, Nat.compare, existing)){
              case(?existing) existing;
              case(null) {
                results.add(?#Err(#GenericError({
                    error_code = 2002;
                    message = "indexed item not found";
                  })));
                  continue proc;
              };
            };
            case(null) {
              let newSub = {
                id = state.nextSubscriptionID;
                initialConfig = thisItem.config;
                publicationId = publication.id;
                namespace = thisItem.namespace;
                controllers = getSubscriptionControllers(caller, configMap);
                stakeIndex = BTree.init<Nat, Principal>(null);
                subscribers = Map.new<Principal, SubscriberRecord>();
              } : SubscriptionRecord;
              fileSubscription(newSub);
              state.nextSubscriptionID += 1;

              let trxid = switch(environment.add_record){
                case (?addRecord) {
                  //todo: calculate value of blocks
                  let txtop = Vector.fromIter<(Text, Value)>([("btype",#Text("72SubReg")),("ts", #Nat(Int.abs(Time.now())))].vals());
                  let tx = Vector.fromIter<(Text, Value)>([
                    ("namespace", #Text(newSub.namespace) : Value) : (Text,Value),
                    ("config", Conversion.CandySharedToValue(#Map(parsedItem.config) : Candy.CandyShared): Value): (Text,Value),
                    ("subscriptionId", #Nat(newSub.id): Value ) : (Text,Value),
                  ].vals());
                  switch(thisItem.memo){
                    case (?memo) {
                      tx.add(("memo", #Blob(memo)));
                    };
                    case (null) {};
                  };
                  addRecord(Vector.toArray(tx), ?Vector.toArray(txtop));
                };
                case (null) 0;
              };

              let actionID = environment.tt.setActionASync<system>(natNow(), {actionType = CONST.publication.actions.assignBroadcasterToSubscriber;
              params = to_candid(publication.namespace, caller, parsedItem.config)}, ONE_MINUTE*5);


              results.add(?#Ok(newSub.id));
              newSub;
            };
          };

          //is the ;
        };

        return Vector.toArray(results);

    };
    

    public func icrc72_update_subscription(caller: Principal, request:[SubscriptionUpdateRequest]) : async* [SubscriptionUpdateResult]{
      //todo: security check

      return await* update_subscription(caller, request, null);
    };

    public func update_subscription<system>(caller: Principal, request:[SubscriptionUpdateRequest], canUpdateSubscription: CanUpdateSubscription) : async* [SubscriptionUpdateResult]{

      let results = Vector.Vector<SubscriptionUpdateResult>();

      label proc for(thisItem : SubscriptionUpdateRequest  in request.vals()){
        
        //see if we already have the publication
        let existing = switch(thisItem.subscription){
          case(#id(val)){
            switch(BTree.get(state.subscriptions, Nat.compare, val)){
              case(null) null;
              case(?existing) ?existing;
            };
          };
          case(#namespace(val)){
            switch(BTree.get(state.subscriptionsByNamespace, Text.compare, val)){
              case(null) null;
              case(?existing) switch(BTree.get(state.subscriptions, Nat.compare,existing)){
                case(null) null;
                case(?existing) ?existing;
              };
            };
          };
        };

        
        switch(existing){
          case (?existing) {
            //make sure we are a controller or no controller 
            let bAuthorized = if(Set.size(existing.controllers) == 0){
              caller == thisItem.subscriber;
            } else {
              switch(Set.contains<Principal>(existing.controllers, Set.phash, caller)){
                case(?val) val;
                case(null) false;
              };
            };
            switch(Set.contains<Principal>(existing.controllers, Set.phash, caller)){
              case(?val){

                let parsedItem : SubscriptionUpdateRequest = switch(canUpdateSubscription){
                  case (?interceptor) {
                    switch(await* interceptor(caller, thisItem)){
                      case (#awaited(item)) item;
                      case (#trappable(item)) item;
                      case (#err(#trappable(err))){
                        results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "trappable: " #debug_show(err);
                        })));
                        continue proc;
                      };
                      case (#err(#awaited(err))){
                        results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "awaited: " # debug_show(err);
                        })));
                        continue proc;
                      };
                    };
                  };
                  case (null) thisItem;
                };
                
                //we need to update the settings, looking for a flag to see if we need to alert a publisher

              
                if(parsedItem.config.0 == CONST.subscription.controllers.list_add){
                  ///MARK: Add CTRl
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          //need to revert here...hmmm
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                  
                  for(thisItem in items){
                    Set.add(existing.controllers, phash, thisItem);
                  };
                    

                } else if (parsedItem.config.0 == CONST.subscription.controllers.list_remove){
                  ///MARK: Rem CTRl
                  let items = switch(parsedItem.config.1){
                    case(#Blob(principal)){
                      [Principal.fromBlob(principal)].vals();
                    };
                    case(#Array(items)){
                      let foundPrincipals = Vector.Vector<Principal>();
                      for(item in items.vals()){
                        let #Blob(principal) = item else{
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid principal";
                          })));
                          continue proc;
                        };
                        foundPrincipals.add(Principal.fromBlob(principal));
                      };
                      foundPrincipals.vals();
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid principal";
                        })));
                        continue proc;
                    };
                  };
                 
                  for(thisItem in items){
                    ignore Set.remove(existing.controllers, phash, thisItem);
                  };
                    
                
                } else if (parsedItem.config.0 == CONST.subscription.filter_update){

                  let ?subscriber = getSubscriberFromUpdate(parsedItem, existing) else {
                      results.add(?#Err(#GenericError({
                        error_code = 3001;
                        message = "subscriber required";
                      })));
                      continue proc;
                  };
                    
                  ///MARK: fltrupdt
                  let #Text(filter) = parsedItem.config.1 else {
                    results.add(?#Err(#GenericError({
                      error_code = 0;
                      message = "Invalid filter";
                    })));
                    continue proc;
                  };
                  subscriber.filter := ?filter;
                } else if (parsedItem.config.0 == CONST.subscription.filter_remove){

                  let ?subscriber = getSubscriberFromUpdate(parsedItem, existing) else {
                      results.add(?#Err(#GenericError({
                        error_code = 3001;
                        message = "subscriber required";
                      })));
                      continue proc;
                  };

                  ///MARK: fltrrmv
                  subscriber.filter := null;
                }else if (parsedItem.config.0 == CONST.subscription.filter_update){

                  let ?subscriber = getSubscriberFromUpdate(parsedItem, existing) else {
                      results.add(?#Err(#GenericError({
                        error_code = 3001;
                        message = "subscriber required";
                      })));
                      continue proc;
                  };
                    
                  ///MARK: skipupdt
                  let (skip, offset) = switch(parsedItem.config.1){
                    case(#Array(val)){
                      if(val.size() == 1){
                        let #Nat(skip) = val[0] else {
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid skip";
                          })));
                          continue proc;
                        };
                        (skip, 0);
                      } else if(val.size() == 2){
                        let #Nat(skip) = val[0] else {
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid skip";
                          })));
                          continue proc;
                        };
                        let #Nat(offset) = val[1] else {
                          results.add(?#Err(#GenericError({
                            error_code = 0;
                            message = "Invalid skip";
                          })));
                          continue proc;
                        };
                        (skip, offset);
                      }else {
                        results.add(?#Err(#GenericError({
                          error_code = 0;
                          message = "Invalid skip";
                        })));
                        continue proc;
                      };
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                        error_code = 0;
                        message = "Invalid skip";
                      })));
                      continue proc;
                    };
                  };
                  subscriber.skip := ?(skip, offset);
                } else if (parsedItem.config.0 == CONST.subscription.filter_remove){

                  let ?subscriber = getSubscriberFromUpdate(parsedItem, existing) else {
                      results.add(?#Err(#GenericError({
                        error_code = 3001;
                        message = "subscriber required";
                      })));
                      continue proc;
                  };

                  ///MARK: skiprmv
                  subscriber.skip := null;
                } else if (parsedItem.config.0 == CONST.subscription.bActive){
                  let ?subscriber = getSubscriberFromUpdate(parsedItem, existing) else {
                      results.add(?#Err(#GenericError({
                        error_code = 3001;
                        message = "subscriber required";
                      })));
                      continue proc;
                  };

                  let active = switch(parsedItem.config.1){
                    case(#Text(val)){
                      if(val == "true"){
                        true;
                      } else {
                        false;
                      };
                    };
                    case(_){
                      results.add(?#Err(#GenericError({
                        error_code = 0;
                        message = "Invalid active";
                      })));
                      continue proc;
                    };
                  };

                  subscriber.bActive := active;

                  //todo: emit subscriber update
                };

                let trxid = switch(environment.add_record){
                  case (?addRecord) {
                    //todo: calculate value of blocks
                    let txtop = Vector.fromIter<(Text, Value)>([("btype",#Text("72PubReg")),("ts", #Nat(Int.abs(Time.now())))].vals());
                    let tx = Vector.fromIter<(Text, Value)>([
                      ("namespace", #Text(existing.namespace) : Value) : (Text,Value),
                      ("config", Conversion.CandySharedToValue(#Map([parsedItem.config]) : Candy.CandyShared): Value): (Text,Value),
                      ("subscriptionId", #Nat(existing.id): Value ) : (Text,Value),
                    ].vals());
                    switch(thisItem.memo){
                      case (?memo) {
                        tx.add(("memo", #Blob(memo)));
                      };
                      case (null) {};
                    };
                    addRecord(Vector.toArray(tx), ?Vector.toArray(txtop));
                  };
                  case (null) 0;
                };

                switch(parsedItem.subscriber){
                  case(null){
                    //emit to any listeners
                    for(listener in subscriptionRegisteredListeners.vals()){
                      listener.1<system>(existing, trxid);
                    };
                  };
                  case(?val)
                  {
                    //emit to any listeners
                    for(listener in subscriberRegisteredListeners.vals()){
                      listener.1<system>(existing, val, trxid);
                    };
                  };
                };
                
              };
              case(null) {
                results.add(?#Err(#Unauthorized));
                continue proc;
              };
            

            
            };
          };
          case (null) {
            results.add(?#Err(#NotFound));
            continue proc;
          };
        };
      };

      return Vector.toArray(results);
    };

    //MARK: Stats

    public func getPublisherStats(publisher: Principal, statsFilter: ?ICRC16Map) : StatsResponse {
      return [];
    };

    public func getPublicationStats(publication: Text, statsFilter: ?ICRC16Map) : StatsResponse {
      return [];
    };

    public func getPublicationPublisherStats(publication: Nat, publisher: Principal, statsFilter: ?ICRC16Map) : StatsResponse {
      return [];
    };

    public func getSubscriberStats(subscriber: Principal, statsFilter: ?ICRC16Map) : StatsResponse {
      return [];
    };

    public func getSubscriptionStats(subscription: Nat, statsFilter: ?ICRC16Map) : StatsResponse {
      return [];
    };

    public func getBroadcasterStats(broadcaster: Principal, statsFilter: ?ICRC16Map) : StatsResponse {
      return [];
    };

    public func getPublicationBroadcasterStats(publication: Nat, broadcaster: Principal, statsFilter: ?ICRC16Map) : StatsResponse {
      return [];
    };

    //Mark: Configs

    public func getPublicationConfigResponse(record: PublicationRecord) : [ICRC16Map] {
      return [];
    };

    public func getPublisherConfigResponse(record: PublisherRecord) : [ICRC16Map] {
      return [];
    };

    public func getSubscriptionSubscriberConfigResponse(subscription: Nat, subscriber: SubscriberRecord) : [ICRC16Map] {
      return [];
    };

    public func getSubscriptionConfigResponse(record: SubscriptionRecord) : [ICRC16Map] {
      return [];
    };

    

    //MARK: Queries

    // Returns the publishers known to the Orchestrator canister

    private func getTake(params: ?Nat) : Nat{
      switch(params){
        case(?val) {
          if(val > state.maxTake){
            state.maxTake;
          } else {
            val;
          };
        };
        case(null) state.defaultTake;
      };
    };

    public func icrc72_get_publishers(params: {
      prev: ?Principal;
      take: ?Nat;
      statsFilter: ??ICRC16Map;
    }): [PublisherInfoResponse] {
      let result = Vector.Vector<PublisherInfoResponse>();
      let start = params.prev;
      let count = getTake(params.take);
      var bFound = switch(params.prev){
        case(?val) false;
        case(null) true;
      };

      label proc for (items in BTree.entries(state.publications)) {
        label procPubs for (publishers in Map.entries(items.1.registeredPublishers)) {
          if (start != null and ?publishers.0 != start) {
            continue procPubs;
          };
          bFound := true;
          result.add({publisher = publishers.0; stats = switch(params.statsFilter){
            case(null) [];
            case(?val) getPublisherStats(publishers.0, val)
            };
          });
          if (result.size() >= count) {
            break procPubs;
          }
        };
      };

      return Vector.toArray(result);
    };

    // Get publications known to the Orchestrator canister
    public func icrc72_get_publications(params: {
      prev: ?Text;
      take: ?Nat;
      statsFilter: ??[(Text, ICRC16)];
    }): [PublicationInfoResponse] {
      let result = Vector.Vector<PublicationInfoResponse>();
      let start = switch(params.prev){
        case(?val) val;
        case(null) "";
      };
      let end = "~";
      let count = getTake(params.take);

      label proc for (item in BTree.scanLimit(state.publicationsByNamespace, Text.compare, start, end, #fwd, count+1).results.vals()) {

        let publication = switch (BTree.get(state.publications, Nat.compare, item.1)) {
          case (?pub) pub;
          case (null) continue proc;
        };

        result.add({
          namespace = item.0;
          publicationId = item.1;
           config = getPublicationConfigResponse(publication);
           stats = switch(params.statsFilter){
            case(null) [];
            case(?val) getPublicationStats(item.0, val);
           };
          });
        if (result.size() >= count) {
          break proc;
        }
      };

      return Vector.toArray(result);
    };

    // Get publications known to the Orchestrator canister
    public func icrc72_get_publication_publishers(params: {
      publication: PublicationIdentifier;
      prev: ?Principal;
      take: ?Nat;
      statsFilter: ??[(Text, ICRC16)];
    }): async* [PublisherPublicationInfoResponse] {
      let result = Vector.Vector<PublisherPublicationInfoResponse>();
      let start = params.prev;
      let count = getTake(params.take);
      var bFound = switch(params.prev){
        case(?val) false;
        case(null) true;
      };

      let publicationId = switch(params.publication){
        case(#publicationId(val)) val;
        case(#namespace(val)) switch (BTree.get(state.publicationsByNamespace, Text.compare, val)) {
          case (?id) id;
          case (null) return [];
        };
      };

      let publicationItem = switch (BTree.get(state.publications, Nat.compare, publicationId)) {
        case (?pub) pub;
        case (null) return [];
      };

      label proc for (item in Map.entries(publicationItem.registeredPublishers)) {
        if (bFound == false and ?item.0 != start) {
          continue proc;
        };
        bFound := true;
        result.add({
          config = getPublisherConfigResponse(item.1);
          stats = switch(params.statsFilter){
            case(null) [];
            case(?val) getPublicationPublisherStats(publicationId, item.0, val);
          };
          publisher = item.0; 
          publicationId = publicationId;
          namespace = publicationItem.namespace;
        });
        if (result.size() >= count) {
          break proc;
        }
      };

      return Vector.toArray(result);
    };

    // Returns the subscribers known to the Orchestrator canister
    public func icrc72_get_subscribers(params: {
      prev: ?Principal;
      take: ?Nat;
      statsFilter: ??[(Text, ICRC16)];
    }): [SubscriberInfoResponse] {
      let result = Vector.Vector<SubscriberInfoResponse>();
      let start = params.prev;
      let count = getTake(params.take);

      var bFound = switch(params.prev){
        case(?val) false;
        case(null) true;
      };

      label proc for (item in BTree.entries(state.subscribersByPrincipal)) {
        if (bFound == false and ?item.0 != start) {
          continue proc;
        };
        result.add({
          subscriber = item.0; 
          stats = switch(params.statsFilter){
            case(null) [];
            case(?val) getSubscriberStats(item.0, val);
          };
        });
        if ( result.size() >= count) {
          break proc;
        }
      };

      return Vector.toArray(result);
    };

    public func icrc72_get_subscriptions(params: {
      prev: ?Nat;
      take: ?Nat;
      statsFilter: ??[(Text, ICRC16)];
    }): [SubscriptionInfoResponse] {
      let result = Vector.Vector<SubscriptionInfoResponse>();
      let start = params.prev;
      let count = getTake(params.take);

      var bFound =  switch(params.prev){
        case(?val) false;
        case(null) true;
      };

      label proc for (item in BTree.entries(state.subscriptions)) {
        if (bFound == false and ?item.0 != start) {
          continue proc;
        };
        bFound := true;
        result.add({
          subscriptionId = item.0;
          namespace = item.1.namespace;
          config = getSubscriptionConfigResponse(item.1);
          stats = switch(params.statsFilter){
            case(null) [];
            case(?val) getSubscriptionStats(item.0, val);
          };
        });
        if (result.size() >=count) {
          break proc;
        }
      };

      return Vector.toArray(result);
    };


    public func icrc72_get_broadcasters(params: {
      prev: ?Principal;
      take: ?Nat;
      statsFilter: ??[(Text, ICRC16)];
    }): [BroadcasterInfoResponse] {
      let result = Vector.Vector<BroadcasterInfoResponse>();
      let start = params.prev;
      let count = getTake(params.take);
      var bFound = switch(params.prev){
        case(?val) false;
        case(null) true;
      };

      label proc for (item in BTree.entries(state.broadcasters)) {
        if (bFound == false and ?item.0 != start) {
          continue proc;
        };
        bFound := true;
        result.add({
          broadcaster = item.0; 
          stats = switch(params.statsFilter){
            case(null) [];
            case(?val) getBroadcasterStats(item.0, val);
          };
        });
        if (result.size() >= count) {
          break proc;
        }
      };

      return Vector.toArray(result);
    };

    public func icrc72_get_publication_broadcasters(params: {
      publication: PublicationIdentifier;
      prev: ?Principal;
      take: ?Nat;
      statsFilter: ??[(Text, ICRC16)];
    }): [BroadcasterInfoResponse] {
      let result = Vector.Vector<BroadcasterInfoResponse>();
      let start = params.prev;
      let count = getTake(params.take);

      var bFound = switch(params.prev){
        case(?val) false;
        case(null) true;
      };

      let publicationId = switch(params.publication){
        case(#publicationId(val)) val;
        case(#namespace(val)) switch (BTree.get(state.publicationsByNamespace, Text.compare, val)) {
          case (?id) id;
          case (null) return [];
        };
      };

      let publicationItem = switch (BTree.get(state.publications, Nat.compare, publicationId)) {
        case (?pub) pub;
        case (null) return [];
      };

      label proc for (publisher in Map.entries(publicationItem.registeredPublishers)) {
        
        label proc for (broadcaster in Set.keys(publisher.1.broadcasters)) {
          if (bFound == false and ?broadcaster != start){
            continue proc;
          } else {
            bFound := true;
          };
          /* let broadcasterInfo = switch (BTree.get(state.broadcasters, Principal.compare, broadcaster)) {
            case (?broad) broad;
            case (null) continue proc;
          }; */
          result.add({
            broadcaster = broadcaster; 
            stats = switch(params.statsFilter){
              case(null) [];
              case(?val) getPublicationBroadcasterStats(publicationId, broadcaster, val);
            };
          });
          if (result.size() >= count) {
            break proc;
          };
        };
      };

      return Vector.toArray(result);
    };

    


    ///MARK: Listeners

    type Listener<T> = (Text, T);

    private let publicationRegisteredListeners = Vector.Vector<(Text, PublicationRegisteredListener)>();

    private let subscriptionRegisteredListeners = Vector.Vector<(Text, SubscriptionRegisteredListener)>();

    private let subscriberRegisteredListeners = Vector.Vector<(Text, SubscriberRegisteredListener)>();

    /// Generic function to register a listener.
      ///
      /// Parameters:
      ///     namespace: Text - The namespace identifying the listener.
      ///     remote_func: T - A callback function to be invoked.
      ///     listeners: Vec<Listener<T>> - The list of listeners.
      public func registerListener<T>(namespace: Text, remote_func: T, listeners: Vector.Vector<Listener<T>>) {
        let listener: Listener<T> = (namespace, remote_func);
        switch(Vector.indexOf<Listener<T>>(listener, listeners, func(a: Listener<T>, b: Listener<T>) : Bool {
          Text.equal(a.0, b.0);
        })){
          case(?index){
            listeners.put(index, listener);
          };
          case(null){
            listeners.add(listener);
          };
        };
      };

    /// `registerPublicationRegisteredListener`
    ///
    /// Registers a new listener or updates an existing one in the provided `listeners` vector.
    ///
    /// Parameters:
    /// - `namespace`: A unique namespace used to identify the listener.
    /// - `remote_func`: The listener's callback function.
    /// - `listeners`: The vector of existing listeners that the new listener will be added to or updated in.
    public func registerPublicationRegisteredListener(namespace: Text, remote_func : PublicationRegisteredListener){
      registerListener<PublicationRegisteredListener>(namespace, remote_func, publicationRegisteredListeners);
    };

    public func registerSubscriptionRegisteredListener(namespace: Text, remote_func : SubscriptionRegisteredListener){
      registerListener<SubscriptionRegisteredListener>(namespace, remote_func, subscriptionRegisteredListeners);
    };

    public func registerSubscriberRegisteredListener(namespace: Text, remote_func : SubscriberRegisteredListener){
      registerListener<SubscriberRegisteredListener>(namespace, remote_func, subscriberRegisteredListeners);
    };


    environment.tt.registerExecutionListenerAsync(?CONST.publication.actions.canAssignBroadcaster, canAssignBroadcaster );
    environment.tt.registerExecutionListenerAsync(?CONST.publication.actions.assignBroadcasterToSubscriber, assignBroadcasterToSubscriber );



  };
}