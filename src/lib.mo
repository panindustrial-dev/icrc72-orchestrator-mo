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
import Buffer "mo:base/Buffer";
import D "mo:base/Debug";
import Error "mo:base/Error";
import Int "mo:base/Int";
import Nat "mo:base/Nat";
import Conversion = "mo:candy/conversion";
import Candy = "mo:candy/types";
import ICRC72Publisher = "../../icrc72-publisher.mo/src/";

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
  public type SubscriptionIndex = MigrationTypes.Current.SubscriptionIndex;

  public type OrchestrationQuerySlice = Service.OrchestrationQuerySlice;
  public type OrchestrationFilter = Service.OrchestrationFilter;
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
  public type ValidBroadcastersResponse = Service.ValidBroadcastersResponse;
  public type InitArgs = MigrationTypes.Current.InitArgs;

  
  public let init = Migration.migrate;
  public let CONST = MigrationTypes.Current.CONST;

  public let Set = MigrationTypes.Current.Set;

  public let Map = MigrationTypes.Current.Map;
  public let {phash; thash; nhash} = Map;
  public let Vector = MigrationTypes.Current.Vector;
  public let BTree = MigrationTypes.Current.BTree;
  


  public let ONE_MINUTE = 60000000000 : Nat; //NanoSeconds

  public func initialState() : State {#v0_0_0(#data)};
  public let currentStateVersion = #v0_1_0(#id);

  type ConfigMap  = Map.Map<Text, ICRC16>;

  public class Orchestrator(stored: ?State, canister: Principal, environment: Environment){

    let debug_channel = {
      var announce = true;
      var setup = true;
    };

    public var governance = MigrationTypes.Current.governance;

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

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: isApprovedPublisher " # debug_show(record));

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

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: isApprovedSubscriber " # debug_show(record));

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
          //todo: add a default case Shouldn't this be only the requester canister?
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

    private func getSubnetForPrincipal(principal: Principal) : async* Star.Star<Principal, Text> {
      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: getSubnetForPrincipal " # debug_show(principal));

      //get subnet
      let #Ok(subnet) = try{await governance.get_subnet_for_canister({principal = ?principal})} catch(e){
        return #err(#awaited("subnet error " # Error.message(e)));
      } else return #err(#awaited("subnet error"));

      let ?subnetPrincipal = subnet.subnet_id else return #err(#awaited("subnet error"));

      return #awaited(subnetPrincipal);
    };

    ///MARK: Asgn BC to Pub
    private func canAssignBroadcaster(actionId: TT.ActionId, action: TT.Action) : async* Star.Star<TT.ActionId, TT.Error> {

      //todo: if there is an error, we need to broadcast to the requesting publisher that the broadcaster could not be assigned
   
      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: canAssignBroadcaster " # debug_show(action));

      if(action.actionType== CONST.publication.actions.canAssignBroadcaster){

          var bAwaited = false;
          let ?params = from_candid(action.params) : ?({id: Nat; canister: Principal}) else return #err(#trappable({error_code = 0; message = "candid error"}));

          debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: canAssignBroadcaster params " # debug_show(params));

          let existing = switch(BTree.get(state.publications, Nat.compare, params.id)){
            case (?existing) existing;
            case(null){
              return #err(#trappable({error_code = 1001; message = "publication not found"}));
            };
          };

          debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: canAssignBroadcaster existing " # debug_show(existing));
        
          let approved = switch(await* isApprovedPublisher(params.canister, existing)){
            case(#trappable(val)) val;
            case(#awaited(val)) {
              bAwaited := true;
              val;
            };
            case(#err(#trappable(err))) return #err(#trappable(err));
            case(#err(#awaited(err))) return #err(#awaited(err));
          };

          debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: canAssignBroadcaster approved " # debug_show(approved));
          if(approved){
            
            D.print(".    ORCHESTRATOR: candidParsed" # debug_show(params));
            let existingPublisher = switch(Map.get<Principal, PublisherRecord>(existing.registeredPublishers, Map.phash, params.canister)){
              case (?publisherRecord) publisherRecord;
              case (null) {
                let newPublisherRecord : PublisherRecord = {
                  //todo: figure out how to pick a broadcaster
                  broadcasters = Set.new<Principal>();
                  var subnet = null;
                };
                ignore Map.put(existing.registeredPublishers, phash, params.canister, newPublisherRecord);
                newPublisherRecord;
              };
            };
            
     
            //need to create the publisher record
            debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: creating publisher record in publication" # debug_show(existing, params.canister));

            //get subnet
            let foundSubnet : Principal = switch(existingPublisher.subnet){
              case(null){
                let #Ok(subnet) = try{await governance.get_subnet_for_canister({principal = ?params.canister})} catch(e){
                  return starError(bAwaited, 2, "subnet error");
                } else return starError(bAwaited, 2, "subnet error");

                debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: canAssignBroadcaster subnet " # debug_show(subnet));

                let ?foundSubnet = subnet.subnet_id else return starError(bAwaited, 2, "empty subnet error");

                existingPublisher.subnet := ?foundSubnet;
                foundSubnet;
              };
              case(?subnet) subnet;
            };

            //do we have a broadcaster on this subnet?
            let foundBroadcaster = switch(Map.get(state.broadcastersBySubnet, phash, foundSubnet)){
              case(?broadcaster) broadcaster;
              case (null) {
                //can we deploy one?
                //todo: Maybe provide a way to deply a broadcaster dynamically
                return starError(bAwaited, 3, "subnet not supported");
              };
            };

            debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: canAssignBroadcaster foundBroadcaster " # debug_show(Vector.toArray(foundBroadcaster)));
            for(thisBroadcaster in Vector.vals(foundBroadcaster)){
              debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: canAssignBroadcaster foundBroadcaster " # debug_show(thisBroadcaster));
              Set.add(existingPublisher.broadcasters, phash, thisBroadcaster);
              debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: canAssignBroadcaster filing broadcaster " # debug_show((thisBroadcaster, existing.namespace)));
              environment.icrc72Publisher.fileBroadcaster(thisBroadcaster, existing.namespace);
            };
            

            

            //todo may not need
            

            //todo: 72BroadcasterAssign


            //emit the event to the broadcaster that it has a new broadcast assignment(this will in turn notify the publisher it is ready to listen)

            debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: canAssignBroadcaster notify broadcasters of new publisher" # debug_show(existingPublisher.broadcasters));

            label notify for(thisBroadcaster in Set.keys(existingPublisher.broadcasters)){
              //who all do we need to notify?
                //the broadcaster
                //the boradcaster in turn notifies the following
                  //the publisher - immediatly
                  //the subscriber - after the subscriber subscribes


              debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: canAssignBroadcaster notify broadcaster of new publisher" # debug_show((thisBroadcaster, existing)));
              ignore environment.icrc72Publisher.publish<system>([{
                namespace = CONST.broadcasters.sys # Principal.toText(thisBroadcaster);
                data = #Map([(
                  CONST.broadcasters.publisher.add, 
                  #Array([
                    #Array([
                      #Text(existing.namespace), 
                      #Blob(Principal.toBlob(params.canister))
                    ] )
                  ]))]);
                headers = null;
              }]);

              /* ignore environment.icrc72Publisher.publish<system>([{
                namespace = CONST.publishers.sys # Principal.toText(params.canister);
                data = #Map([(
                  CONST.broadcasters.publisher.broadcasters.add, 
                  #Array([
                    #Array([
                      #Text(existing.namespace), 
                      #Blob(Principal.toBlob(thisBroadcaster))
                    ] )
                  ]))]);
                headers = null;
              }]); */
            };
              



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
      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber " # debug_show(action));

      //todo: if there is an error, we need to broadcast to the requesting subscriber  that the broadcaster could not be assigned

      let foundStake = 0; //todo: figure out staking
   
      
      if(action.actionType== CONST.publication.actions.assignBroadcasterToSubscriber){

          var bAwaited = false;

          let ?params = from_candid(action.params) : ?({id: Nat; canister: Principal; config: ICRC16Map}) else return starError(bAwaited, 1, "candid error");

          debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber params " # debug_show(params));

          let existingSubscription = switch(BTree.get(state.subscriptions, Nat.compare, params.id)){
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

          let approved = switch(await* isApprovedSubscriber(params.canister, existingPublication)){
            case(#trappable(val)) val;
            case(#awaited(val)) {
              bAwaited := true;
              val;
            };
            case(#err(#trappable(err))) return #err(#trappable(err));
            case(#err(#awaited(err))) return #err(#awaited(err));
          };

          debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber approved " # debug_show(existingPublication, existingSubscription, approved));

          if(approved){

            debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber approved ");

            let configMap = Map.fromIter<Text, ICRC16>(params.config.vals(), thash);

            let existingSubscriber = switch(Map.get<Principal, SubscriberRecord>(existingSubscription.subscribers, Map.phash, params.canister)){
              case (?subscriberRecord) subscriberRecord;
              case (null) {
                debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: creating subscriber record in subscription" # debug_show(existingSubscription, params.canister));
                //need to create the subscriber record
                let newSubscriber  : SubscriberRecord = {
                  subscriptionId = existingSubscription.id;
                  initialConfig = existingSubscription.initialConfig;
                  var stake = 0;
                  var filter = getSubFilter(configMap);
                  var bActive = getSubActive(configMap);
                  var skip = getSubSkip(configMap);
                  subscriber = params.canister;
                  var subnet = null;
                  registeredBroadcasters = Set.new<Principal>();
                };

                fileSubscriber(existingSubscription, newSubscriber);
                
                newSubscriber;
                
              };
            };

            //get subnet
            let subscriberSubnet = switch(existingSubscriber.subnet){
              case(null){
                let #awaited(subnet) = await* getSubnetForPrincipal(params.canister) else return starError(bAwaited, 2, "subnet error");
                subnet;
              };
              case(?subnet) subnet;
            };

            let thisSubnetBroadcaster : Vector.Vector<Principal> = switch(Map.get(state.broadcastersBySubnet, phash, subscriberSubnet)){
              case(?broadcaster) broadcaster;
              case (null) {
                //todo: can we deploy one?
                //todo; emit errors
                return starError(bAwaited, 3, "subnet not supported");
              };
            };

            let selectedBroadcaster = switch(Vector.getOpt(thisSubnetBroadcaster, 0)){
              case(?val) val;
              case (null) return starError(bAwaited, 3, "broadcaster not found");
            };

            debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber selectedBroadcaster " # debug_show(selectedBroadcaster));

            debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: handling relays " # debug_show(selectedBroadcaster));


            debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: pulishers to scan " # debug_show(Map.toArray(existingPublication.registeredPublishers)));

            //For every publisher that is not on the same subnet we need to add a relay to that publisher's broadcaster's
            for(publisher in Map.entries(existingPublication.registeredPublishers)){
              debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber testing subnet " # debug_show((publisher.1.subnet, subscriberSubnet)));
              if(publisher.1.subnet != ?subscriberSubnet){
                debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber adding relay to publisher " # debug_show(publisher.1));
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
                  
                 
                  let foundRelaySet = switch(Map.get(foundRelay.0, phash, selectedBroadcaster)) {
                    case(?relaySet) relaySet;
                    case (null) {
                      let newRelaySet = Set.new<Principal>();
                      ignore Map.put(foundRelay.0, Map.phash, selectedBroadcaster, newRelaySet);
                      newRelaySet;
                    };
                  };

                  //assign the relay
                  for(thisRelay in Set.keys(foundRelaySet)){
                    ignore environment.icrc72Publisher.publish<system>([{
                      namespace = CONST.broadcasters.sys # Principal.toText(selectedBroadcaster);
                      data = #Map([(CONST.broadcasters.relay.add, 
                        #Array([
                          #Array([
                            #Text(existingPublication.namespace), 
                            #Blob(Principal.toBlob(thisRelay))
                          ] )
                        ]))
                      ]);
                      headers = null;
                    }]);
                  };

                  //todo: 72SubscritionAssign

                  //add to the Main Dictionary
                  Set.add(foundRelaySet, phash, params.canister);

                  //add to the stake index
                  ignore BTree.insert(foundRelay.1, Nat.compare, foundStake, (selectedBroadcaster, params.canister));

                  //add to the registered broadcasters
                  Set.add(existingSubscriber.registeredBroadcasters, phash, selectedBroadcaster);
                };
              } else {
                //add this subscriber to the broadcaster's list

                debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber adding subscriber to broadcaster " # debug_show(selectedBroadcaster));


                let foundBroadcaster = switch(BTree.get(state.broadcasters, Principal.compare, selectedBroadcaster)){
                  case(?broadcaster) broadcaster;
                  case (null) {
                    //can we deploy one? should be unreachable
                    //todo; emit errors
                    return starError(bAwaited, 3, "broadcaster not found");
                  };
                };

                debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber foundBroadcaster " # debug_show(foundBroadcaster));

                let foundNamespace = switch(Map.get(foundBroadcaster.subscribers, thash, existingPublication.namespace)){
                  case(?namespace) namespace;
                  case (null) {
                    let newNamespaceRecord = (Set.new<Principal>(), BTree.init<Nat, Principal>(null));
                    ignore Map.put(foundBroadcaster.subscribers, Map.thash, existingPublication.namespace, newNamespaceRecord );
                    newNamespaceRecord;
                  };
                };

                debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber foundNamespace " # debug_show(foundNamespace, params.canister, selectedBroadcaster));

                //todo: 72SubscritionAssign
                Set.add(foundNamespace.0, phash, params.canister);
                ignore BTree.insert(foundNamespace.1, Nat.compare, existingSubscriber.stake, params.canister);
                Set.add(existingSubscriber.registeredBroadcasters, Set.phash, selectedBroadcaster);
                
                debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: assignBroadcasterToSubscriber foundNamespace " # debug_show((existingPublication.namespace, params.canister, selectedBroadcaster)));
                //assign the subscriber
                ignore environment.icrc72Publisher.publish<system>([{
                  namespace = CONST.broadcasters.sys # Principal.toText(selectedBroadcaster);
                  data = #Map([(
                    CONST.broadcasters.subscriber.add, 
                    #Array([
                      #Array([
                        #Text(existingPublication.namespace), 
                        #Blob(Principal.toBlob(params.canister))
                      ] )
                    ])
                  )]);
                  headers = null;
                }]);
              };
            };

            //todo: emit the event to the subscriber that it has an assigned broadcaster
            //todo: emit the event to the broadcaster that it has an assigned relay or subscriber

            /* ignore environment.icrc72Publisher.publish<system>([{
                namespace = CONST.broadcasters.sys # Principal.toText(params.canister);
                data = #Map([(
                  CONST.broadcasters.subscriber.add, 
                  #Array([
                    #Array([
                      #Text(existingSubscription.namespace), 
                      #Blob(Principal.toBlob(existingSubscriber.subscriber))
                    ] )
                  ]))]);
                headers = null;
              }]);
 */
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
      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: icrc72_register_publication " # debug_show(request));
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
        case(null) Set.fromIter<Principal>([caller].vals(), phash);
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
      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_publication  from orchestrator" # debug_show(request));

      let results = Buffer.Buffer<PublicationRegisterResult>(1);

      label proc for(thisItem : PublicationRegistration  in request.vals()){

        debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_publication  from orchestrator" # debug_show(thisItem));
        
        //see if we already have the publication
        let existing = switch(BTree.get(state.publicationsByNamespace, Text.compare, thisItem.namespace)){
          case(null) null;
          case(?val) switch(BTree.get(state.publications, Nat.compare, val)){
            case(null) null;
            case(?existing) ?existing;
          };
        };

        debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_publication existing " # debug_show(existing));

        switch(existing){
          case (?existing) {
        
            //todo: do we want to make sure the type matches? someone could add a bunch of timers here...we will need to add a cool down
            //this publicatin exists, but a new publisher is registering it so we may need to assign a brodcaster and let them know...but we need to know they have permissions, so we'll set that up via the timer tool.
            debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: Publication Registered setting canAssign Broadcaster for existing: " # debug_show((existing.id, caller)));

            //todo: we need this to run inline

            ignore await* canAssignBroadcaster({id = 0; time=0},{aSync = null; actionType = CONST.publication.actions.canAssignBroadcaster;
              params= to_candid({id = existing.id; canister = caller;} );
              retries = 0;} );

            //todo: figure out why we can't do this async...may need event system
            /* ignore environment.tt.setActionASync<system>(natNow(), {actionType = CONST.publication.actions.canAssignBroadcaster; params= to_candid({id = existing.id; canister = caller;})}, ONE_MINUTE * 5);
            results.add(?#Ok(existing.id)); */

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

        debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_publication parsedItem " # debug_show(parsedItem));

        //do we need to gate this somehow? Likely yes : TODO: gate canAddPublication
        let newPublication ={
          id = state.nextPublicationID;
          namespace = parsedItem.namespace; // The namespace of the publication
          initialConfig = parsedItem.config; // Additional configuration or metadata about the publication
          var allowedPublishers = getPublicationPublisherPermission(configMap); // List of publishers allowed to publish under this namespace
          var allowedSubscribers = getPublicationSubscriberPermission(configMap); // List of subscribers allowed to subscribe to this namespace
          registeredPublishers = Map.new<Principal, PublisherRecord>(); // Map of publishers registered and their assigned broadcasters

          subnetIndex = Map.new<Principal, Principal>(); // Map of publishers registered and their assigned broadcasters
          var eventCount = 0;
          var eventsSent  = 0;
          var notifications  = 0;
          var confrimations  = 0;
          controllers = getPublicationControllers(caller, configMap); // List of controllers for the publication
          } : PublicationRecord;

        ignore BTree.insert(state.publications, Nat.compare, newPublication.id, newPublication);
        ignore BTree.insert(state.publicationsByNamespace, Text.compare, newPublication.namespace, newPublication.id);

        //todo: maybe need to check allowed
        ignore Map.put(newPublication.registeredPublishers, Map.phash, caller, {broadcasters = Set.new<Principal>(); var subnet = null} : PublisherRecord);

        debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: Publication Registered " # debug_show(BTree.toArray(state.publications)));
         debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: Publication Registered " # debug_show(BTree.toArray(state.publicationsByNamespace )));

        results.add(?#Ok(state.nextPublicationID));

        state.nextPublicationID += 1;

        let trxid = switch(environment.addRecord){
          case (?addRecord) {
            //todo: calculate value of blocks
            let txtop = Buffer.fromIter<(Text, Value)>([("btype",#Text("72PubReg")),("ts", #Nat(natNow()))].vals());
            let tx = Buffer.fromIter<(Text, Value)>([
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
            addRecord(Buffer.toArray(tx), ?Buffer.toArray(txtop));
          };
          case (null) 0;
        };

        //emit to any listeners
        for(listener in publicationRegisteredListeners.vals()){
          listener.1<system>(newPublication, trxid);
        };

        debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: Publication Registered setting canAssign Broadcaster for new: " # debug_show((newPublication.id, caller, to_candid((newPublication.id, caller)))));

        ignore await* canAssignBroadcaster({id = 0; time=0},{aSync = null; actionType = CONST.publication.actions.canAssignBroadcaster;
              params= to_candid({id = newPublication.id; canister = caller;} );
              retries = 0;} );

              //todo: why can't we do this async
        
        /* ignore environment.tt.setActionASync<system>(natNow(), {actionType = CONST.publication.actions.canAssignBroadcaster; params= to_candid({id = newPublication.id; canister=caller;})}, ONE_MINUTE * 5); */
      };



      return Buffer.toArray(results);
    };

    public func icrc72_update_publication(caller: Principal, request:[PublicationUpdateRequest]) : async* [PublicationUpdateResult]{
      //todo: security check

      return await* update_publication(caller, request, null);
    };

    public func update_publication<system>(caller: Principal, request:[PublicationUpdateRequest], canUpdatePublication: CanUpdatePublication) : async* [PublicationUpdateResult]{

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: update_publication  from orchestrator" # debug_show(request));

      let results = Buffer.Buffer<PublicationUpdateResult>(1);

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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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

                let trxid = switch(environment.addRecord){
                  case (?addRecord) {
                    //todo: calculate value of blocks
                    let txtop = Buffer.fromIter<(Text, Value)>([("btype",#Text("72PubReg")),("ts", #Nat(Int.abs(Time.now())))].vals());
                    let tx = Buffer.fromIter<(Text, Value)>([
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
                    addRecord(Buffer.toArray(tx), ?Buffer.toArray(txtop));
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

      return Buffer.toArray(results);
    };

    public func icrc72_register_subscription(caller: Principal, request:[SubscriptionRegistration]) : async* [SubscriptionRegisterResult]{
      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: icrc72_register_subscription " # debug_show(request));
      return await* register_subscription(caller, request, null);
    };

    private func fileSubscription(subscription: SubscriptionRecord) : SubscriptionIndex {
      
      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: fileSubscription " # debug_show(subscription));

      let existing = switch(BTree.get(state.subscriptions, Nat.compare, subscription.id)){
        case (?existing){
          //todo: we may need to add something here so that we check prevous stake and update it.
          existing;
        };
        case (null) {
          ignore BTree.insert(state.subscriptions, Nat.compare, subscription.id, subscription);
          subscription;
        };
      };

      let existingNamespace = switch(BTree.get(state.subscriptionsByNamespace, Text.compare, subscription.namespace)){
        case(?existing) existing;
        case(null) {
          let newCol =(Map.new<Principal, Nat>(), BTree.init<Nat,Nat>(null));
          ignore BTree.insert(state.subscriptionsByNamespace, Text.compare, subscription.namespace, newCol);
          newCol;
        };
      };

    };

    private func fileSubscriber(subscription: SubscriptionRecord, subscriber: SubscriberRecord) : () {

      //todo: we may need to add something here so that we check prevous stake and update it.  

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: fileSubscriber " # debug_show(subscription, subscriber));
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

      let existingIndex : SubscriptionIndex = switch(BTree.get(state.subscriptionsByNamespace, Text.compare, subscription.namespace)){
        case(?existing) existing;
        case(null) {
          let newCol =(Map.new<Principal, Nat>(), BTree.init<Nat,Nat>(null));
          ignore BTree.insert(state.subscriptionsByNamespace, Text.compare, subscription.namespace, newCol);
          newCol;
        };
      };
      

      ignore BTree.insert(existingIndex.1, Nat.compare, subscriber.stake, subscription.id);
      ignore Map.put(existingIndex.0, phash, subscriber.subscriber, subscription.id);
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

    public func getSubscriptionStake(caller: Principal, namespace: Text, config: ICRC16Map) : async* Nat{
     return 0;
    };

    public func register_subscription(caller: Principal, request:[SubscriptionRegistration], canAddSubscription: CanAddSubscription) : async* [SubscriptionRegisterResult]{
      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_subscription from Orchestrator " # debug_show(request));
        
        let results = Buffer.Buffer<SubscriptionRegisterResult>(1);

        var bAwaited = false;
  
        label proc for(thisItem : SubscriptionRegistration  in request.vals()){

          if(thisItem.namespace == (CONST.subscribers.sys # Principal.toText(caller)) or
          thisItem.namespace == (CONST.publishers.sys # Principal.toText(caller)) or
          thisItem.namespace == (CONST.broadcasters.sys # Principal.toText(caller))){
            let process = switch(BTree.get(state.publicationsByNamespace, Text.compare, thisItem.namespace)){
              case(null) true;
              case(?val) switch(BTree.get(state.publications, Nat.compare, val)){
                case(null) true;
                case(?existing) false;
              };
            }; 

            if(process){
              debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: found valid sys subscription " # debug_show(thisItem));
              //todo: shouldn't I check the publications first
              
              let registrationResult = await* register_publication(canister : Principal, 
              [{
                namespace = thisItem.namespace;
                config = [
                  (
                    CONST.publication.publishers.allowed.list : Text, 
                    #Array([
                      #Blob(Principal.toBlob(canister))
                    ]) : ICRC16
                  ),
                  (
                    CONST.publication.subscribers.allowed.list : Text, 
                    #Array([
                      #Blob(Principal.toBlob(caller))
                    ]) :ICRC16
                  ),
                ] : ICRC16Map;
                memo = null;
                
              } : PublicationRegistration],
              null);

              debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: registrationResult " # debug_show(registrationResult));
            };
            
          };

          

          let ?publication = switch(BTree.get(state.publicationsByNamespace, Text.compare, thisItem.namespace)){
            case(null) null;
            case(?val) switch(BTree.get(state.publications, Nat.compare, val)){
              case(null) null;
              case(?existing) ?existing;
            };
          } else {
            //todo: what if I want to register a subscription for a publication that doesn't exist yet?

            debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_subscription publication not found " # debug_show(thisItem.namespace));
            results.add(?#Err(#PublicationNotFound));
            continue proc;
          };

          //todo: This can be an async call whic will really slow things down...we need to figure out how to handle this with Tokens and a permission in the title or a cached item.
          let approval = switch(await* isApprovedSubscriber(caller, publication)){
            case(#trappable(val)) val;
            case(#awaited(val)) val;
            case(#err(#trappable(err))){
              debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_subscription isApprovedSubscriber trappable error unauthorized " # debug_show(err));
              results.add(?#Err(#Unauthorized));
              continue proc;
            };
            case(#err(#awaited(err))){
              debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_subscription isApprovedSubscriber awaited error unauthorized " # debug_show(err));
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
                  debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_subscription trappable error canAddSubscription " # debug_show(err));
                  results.add(?#Err(#GenericError({
                    error_code = 0;
                    message = "trappable: " #debug_show(err);
                  })));
                  continue proc;
                };
                case (#err(#awaited(err))){
                  debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_subscription awaited error canAddSubscription " # debug_show(err));
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
          let existingId = switch(BTree.get(state.subscriptionsByNamespace, Text.compare, thisItem.namespace)){
            case(?existing){
              switch(Map.get(existing.0, phash, caller)){
                case(null) null;
                case(?existing) ?existing;
              };
            };
            case(null) null;
          };

          let existing = switch(existingId){
            case(?existing) switch(BTree.get(state.subscriptions, Nat.compare, existing)){
              case(?existing) existing;
              case(null) {
                debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_subscription subscription not found " # debug_show(thisItem.namespace));
                results.add(?#Err(#GenericError({
                    error_code = 2002;
                    message = "indexed item not found";
                  })));
                  continue proc;
              };
            };
            case(null) {
              debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_subscription subscription not found creating " # debug_show(thisItem.namespace));

              //todo: needs to have a try/catch wrapper...asses state change
              let newSub = {
                id = state.nextSubscriptionID;
                initialConfig = thisItem.config;
                publicationId = publication.id;
                namespace = thisItem.namespace;
                controllers = getSubscriptionControllers(caller, configMap);
                var stake = await* getSubscriptionStake(caller, thisItem.namespace, thisItem.config);
                subscribers = Map.new<Principal, SubscriberRecord>();
              } : SubscriptionRecord;
              
              let subscriptionIndex = fileSubscription(newSub);
              state.nextSubscriptionID += 1;

              //todo: add the subscriber? maybe need to check and see if they are on the allowed list?
              
             

              let trxid = switch(environment.addRecord){
                case (?addRecord) {
                  //todo: calculate value of blocks
                  let txtop = Buffer.fromIter<(Text, Value)>([("btype",#Text("72SubReg")),("ts", #Nat(Int.abs(Time.now())))].vals());
                  let tx = Buffer.fromIter<(Text, Value)>([
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
                  addRecord(Buffer.toArray(tx), ?Buffer.toArray(txtop));
                };
                case (null) 0;
              };

              debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: register_subscription subscription dircectly calling task" # debug_show(newSub));

              

              ignore await* assignBroadcasterToSubscriber({id = 0; time=0},{aSync = null; actionType = CONST.publication.actions.assignBroadcasterToSubscriber;
              params = to_candid({id = newSub.id; canister = caller; config = parsedItem.config;}); retries = 0;} );

              //let actionID = environment.tt.setActionASync<system>(natNow(), {actionType = CONST.publication.actions.assignBroadcasterToSubscriber;
              //params = to_candid({id = newSub.id; canister = caller; config = parsedItem.config;})}, ONE_MINUTE*5);


              results.add(?#Ok(newSub.id));
              newSub;
            };
          };

          //is the ;
        };

        return Buffer.toArray(results);

    };
    

    public func icrc72_update_subscription(caller: Principal, request:[SubscriptionUpdateRequest]) : async* [SubscriptionUpdateResult]{
      //todo: security check

      return await* update_subscription(caller, request, null);
    };

    public func update_subscription<system>(caller: Principal, request:[SubscriptionUpdateRequest], canUpdateSubscription: CanUpdateSubscription) : async* [SubscriptionUpdateResult]{

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: update_subscription " # debug_show(request));

      let results = Buffer.Buffer<SubscriptionUpdateResult>(1);

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
              case(?existing) switch(Map.get(existing.0, phash, caller)){
                case(null) null;
                case(?existing) switch(BTree.get(state.subscriptions, Nat.compare, existing)){
                  case(null) null;
                  case(?existing) ?existing;
                };
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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
                      let foundPrincipals = Buffer.Buffer<Principal>(1);
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

                let trxid = switch(environment.addRecord){
                  case (?addRecord) {
                    //todo: calculate value of blocks
                    let txtop = Buffer.fromIter<(Text, Value)>([("btype",#Text("72PubReg")),("ts", #Nat(Int.abs(Time.now())))].vals());
                    let tx = Buffer.fromIter<(Text, Value)>([
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
                    addRecord(Buffer.toArray(tx), ?Buffer.toArray(txtop));
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

      return Buffer.toArray(results);
    };

    //MARK: Stats

    public func getPublisherStats(publisher: Principal, statsFilter: ?[Text]) : StatsResponse {
      return [];
    };

    public func getPublicationStats(publication: Text, statsFilter: ?[Text]) : StatsResponse {
      return [];
    };

    public func getPublicationPublisherStats(publication: Nat, publisher: Principal, statsFilter: ?[Text]) : StatsResponse {
      return [];
    };

    public func getSubscriberStats(subscriber: Principal, statsFilter: ?[Text]) : StatsResponse {
      return [];
    };

    public func getSubscriptionStats(subscription: SubscriptionRecord, statsFilter: ?[Text]) : StatsResponse {
      return [];
    };

    public func getBroadcasterStats(broadcaster: Principal, statsFilter: ?[Text]) : StatsResponse {
      return [];
    };

    public func getPublicationBroadcasterStats(publication: Nat, broadcaster: Principal, statsFilter: ?[Text]) : StatsResponse {
      return [];
    };

    //Mark: Configs

    public func getPublicationConfigResponse(record: PublicationRecord) : ICRC16Map {
      return [];
    };

    public func getPublisherConfigResponse(record: PublisherRecord) : ICRC16Map {
      return [];
    };

    public func getSubscriberConfigResponse(subscriber: SubscriberRecord) : ICRC16Map {
      let config = Map.new<Text, ICRC16>();
      ignore Map.put(config, thash, "stake", #Nat(subscriber.stake));
      switch(subscriber.filter){
        case(?val) ignore Map.put(config, thash, "filter", #Text(val));
        case(null) {};
      };
      switch(subscriber.skip){
        case(?(skip, offset)) ignore Map.put(config, thash, "skip", #Array([#Nat(skip), #Nat(offset)]));
        case(null) {};
      };
      return Map.toArray(config);
    };

    public func getSubscriptionConfigResponse(record: SubscriptionRecord) : ICRC16Map {
      //todo: do we need to add more here like stake?
      let config = Map.fromIter<Text, ICRC16>(record.initialConfig.vals(), thash);
      ignore Map.put(config, thash, "stake", #Nat(record.stake));

      return Map.toArray(config);
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

    public func fileBroadcaster(broadcaster: Principal) : async* Bool{
      debug if(debug_channel.setup) D.print(".    ORCHESTRATOR: fileBroadcaster in orchestrator" # debug_show(broadcaster));

      let subnet = try{
        switch(await* getSubnetForPrincipal(broadcaster)){
          case(#trappable(val)) val;
          case(#awaited(val)) val;
          case(#err(#trappable(err))){
            debug if(debug_channel.setup) D.print(".    ORCHESTRATOR: fileBroadcaster subnet error " # debug_show(err));
            return false;
          };
          case(#err(#awaited(err))){
            debug if(debug_channel.setup) D.print(".    ORCHESTRATOR: fileBroadcaster subnet error " # debug_show(err));
            return false;
          };
        };
      } catch(e){
        debug if(debug_channel.setup) D.print(".    ORCHESTRATOR: fileBroadcaster subnet error " # Error.message(e));
        return false;
      };

      debug if(debug_channel.setup) D.print(".    ORCHESTRATOR: fileBroadcaster subnet " # debug_show(subnet));
      
      
      let broadcasterSet = switch(Map.get(state.broadcastersBySubnet, phash, subnet)){
        case (?val) val;
        case (null) {
          let newSet = Vector.new<Principal>();
          ignore Map.put(state.broadcastersBySubnet, phash, subnet, newSet);
          newSet;
        };
      };

      Vector.add(broadcasterSet, broadcaster);

      let broacasterRecord = {
        publishers = Map.new<Principal, Set.Set<Text>>();
        subscribers = Map.new<Text,(Set.Set<Principal>, BTree.BTree<Nat, Principal>)>();
        relays =  Map.new<Text, (Map.Map<Principal, Set.Set<Principal>>, BTree.BTree<Nat, (Principal,Principal)>)>();
        subnet = subnet;
      } : BroadcasterRecord;

      ignore BTree.insert(state.broadcasters, Principal.compare, broadcaster, broacasterRecord);

      //declare publications
      debug if(debug_channel.setup) D.print(".    ORCHESTRATOR: fileBroadcaster filing with publisher " # debug_show(broadcaster));
      environment.icrc72Publisher.fileBroadcaster(broadcaster, CONST.broadcasters.sys # Principal.toText(broadcaster));

      debug if(debug_channel.setup) D.print(".    ORCHESTRATOR: fileBroadcaster publishers " # debug_show(BTree.toArray(environment.icrc72Publisher.getState().broadcasters)));

      debug if(debug_channel.setup) D.print(".    ORCHESTRATOR: currentpublications  " # debug_show(BTree.toArray(state.publications)));

      

      let pubisherResult = await* environment.icrc72Publisher.registerPublications([{
        namespace = CONST.broadcasters.sys # Principal.toText(broadcaster);
        config = [
          (
            CONST.publication.publishers.allowed.list : Text, 
            #Array([
              #Blob(Principal.toBlob(canister))
            ]) : ICRC16
          ),
          (
            CONST.publication.subscribers.allowed.list : Text, 
            #Array([
              #Blob(Principal.toBlob(canister))
            ]) :ICRC16
          ),
        ] : ICRC16Map;
        memo = null;
      }]);

      
      

      

      debug if(debug_channel.setup) D.print(".    ORCHESTRATOR: fileBroadcaster registerPublications " # debug_show(pubisherResult));

      return true;
    };

    public func icrc72_get_valid_broadcaster(caller: Principal) : async* ValidBroadcastersResponse {

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_valid_broadcaster " # debug_show(caller));
       

      let #awaited(subnet) = await* getSubnetForPrincipal(caller) else return #list([]);

      let validBroadcasters = switch(Map.get(state.broadcastersBySubnet, phash, subnet)){
        case (?val) val;
        case (null) return #list([]);
      };

      return #list(Vector.toArray(validBroadcasters));
    };

    public func icrc72_get_publishers(caller: Principal, params: {
      prev: ?Principal;
      take: ?Nat;
      filter: ?OrchestrationFilter;
    }): [PublisherInfoResponse] {
      let result = Buffer.Buffer<PublisherInfoResponse>(1);
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
          result.add({publisher = publishers.0; stats = switch(params.filter){
            case(null) [];
            case(?val) {
              switch(val.statistics){
                case(?val) getPublisherStats(publishers.0, val);
                case(null) [];
              };
            };
          };
          });
          if (result.size() >= count) {
            break procPubs;
          }
        };
      };

      return Buffer.toArray(result);
    };

    private func processSlices(slices : [OrchestrationQuerySlice]) :  {
        namespace : ?Text;
        subscriber : ?Principal;
        broadcaster : ?Principal;
        publisher : ?Principal;
      }{
        var namespace : ?Text = null;
        var subscriber : ?Principal = null;
        var broadcaster : ?Principal = null;
        var publisher : ?Principal = null;

        for(slice in slices.vals()){
          switch(slice){
            case(#ByNamespace(val)){
              namespace := ?val;
            };
            case(#BySubscriber(val)){
              subscriber := ?val;
            };
            case(#ByBroadcaster(val)){
              broadcaster := ?val;
            };
            case(#ByPublisher(val)){
              publisher := ?val;
            };
          };
        };

        return {
          namespace = namespace;
          subscriber = subscriber;
          broadcaster = broadcaster;
          publisher = publisher;
        };
      };

    // Get publications known to the Orchestrator canister
    public func icrc72_get_publications(caller: Principal, params: {
      prev: ?Text;
      take: ?Nat;
      filter: ?OrchestrationFilter;
    }): [PublicationInfoResponse] {

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_publications " # debug_show(params));
      let result = Buffer.Buffer<PublicationInfoResponse>(1);
      
      let count = getTake(params.take);
      let filter = switch(params.filter){
        case(?filter){
          processSlices(filter.slice);
        };
        case(null) processSlices([]);
      };

      var start= switch(params.prev){
        case(?val) val;
        case(null) "";
      };
      var end = "~";

      switch(filter.namespace){
          case(null){};
          case(?namespace){
            start := namespace;
            end := namespace;
          };
        };


      //todo: optimize the slice scan with start/end
      label proc for (item in BTree.scanLimit(state.publicationsByNamespace, Text.compare, start, end, #fwd, count+1).results.vals()) {

        let publication = switch (BTree.get(state.publications, Nat.compare, item.1)) {
          case (?pub) pub;
          case (null) continue proc;
        };

        switch(filter.publisher){
          case(null){};
          case(?publisher){
            switch(Map.get(publication.registeredPublishers, Set.phash, publisher)){
              case(null) continue proc;
              case(?val){};
            };
          };
        };

        switch(filter.subscriber){
          case(null){};
          case(?subscriber){
            switch(BTree.get(state.subscriptionsByNamespace, Text.compare, publication.namespace)){
              case(null) continue proc;
              case(?val){
                switch(Map.get(val.0, Set.phash, subscriber)){
                  case(null) continue proc;
                  case(?val){};
                };
              };
            };
          };
        };

        //todo: may need another index for broadcasters
        /* switch(filter.broadcaster){
          case(null){};
          case(?broadcaster){
            switch(Map.get(publication.broadcastersByPrincipal, Set.phash, broadcaster)){
              case(null) continue proc;
              case(?val){};
            };
          };
        }; */

        result.add({
          namespace = item.0;
          publicationId = item.1;
           config = getPublicationConfigResponse(publication);
           stats = switch(params.filter){
            case(?filter){
              switch(filter.statistics){
                case(null) [];
                case(?val) getPublicationStats(item.0, val);
              };
            };
            case(null) [];
           };
          });
        if (result.size() >= count) {
          break proc;
        }
      };

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_publications result " # debug_show(Buffer.toArray(result)));

      return Buffer.toArray(result);
    };

    /* // Get publications known to the Orchestrator canister
    public func icrc72_get_publication_publishers(params: {
      publication: PublicationIdentifier;
      prev: ?Principal;
      take: ?Nat;
      statsFilter: ??[Text];
    }): async* [PublisherPublicationInfoResponse] {
      let result = Buffer.Buffer<PublisherPublicationInfoResponse>(1);
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

      return Buffer.toArray(result);
    }; */

    // Returns the subscribers known to the Orchestrator canister
    public func icrc72_get_subscribers(caller: Principal, params: {
      prev: ?Principal;
      take: ?Nat;
      filter: ?OrchestrationFilter;
    }): [SubscriberInfoResponse] {

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_subscribers " # debug_show(params));

      let result = Buffer.Buffer<SubscriberInfoResponse>(1);
      let start = params.prev;
      let count = getTake(params.take);

      var bFound = switch(params.prev){
        case(?val) false;
        case(null) true;
      };

      let (slice, statistics) = switch(params.filter){
        case(null) ([],null);
        case(?val) (val.slice, val.statistics);
      };

      let filter = processSlices(slice);

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_subscribers about to loo" # debug_show((filter, BTree.toArray(state.subscriptions))));

      label proc for (item in BTree.entries(state.subscriptions)) {
        

        switch(filter.namespace){
          case(null){};
          case(?namespace){
            if(item.1.namespace != namespace){
              continue proc;
            };
          };
        };

        label procSubscriber for(thisSubscriber in Map.entries(item.1.subscribers)){
          if (bFound == false and ?thisSubscriber.0 != start) {
            continue proc;
          };
          switch(filter.subscriber){
            case(null){};
            case(?subscriber){
              if(subscriber != thisSubscriber.0){
                continue procSubscriber;
              };
            };
          };

          switch(filter.broadcaster){
            case(null){};
            case(?broadcaster){
              if(Set.has(thisSubscriber.1.registeredBroadcasters, Set.phash, broadcaster) == false){
                continue procSubscriber;
              };
            };
          };

          result.add({
            subscriber = thisSubscriber.0;
            subscriptions = switch(filter.namespace){
              case(null) null;
              case(?val) ?[item.1.id];
            };
            config = switch(filter.namespace){
              case(null)[];
              case(?val) getSubscriberConfigResponse(thisSubscriber.1);
            };
            stats =
              switch(statistics){
                case(null) [];
                case(?stats) getSubscriberStats(thisSubscriber.0, stats);
              };
          });

          if ( result.size() >= count) {
            break proc;
          }
        };
      };
        /* };
        case(?#ByNamespace(textfilter)){
          label proc for(thisNamespace in textfilter.vals()){
            let ?subscriptionPointer = BTree.get(state.subscriptionsByNamespace, Text.compare, thisNamespace) else continue proc;
            for(thisSub in Map.entries(subscriptionPointer.0)){
              let ?subscription= BTree.get(state.subscriptions, Nat.compare, thisSub.1) else continue proc;
              //todo: implement proper filter and paging
              for(thisSubscriber in Map.entries(subscription.subscribers)){
                result.add({
                subscriber = thisSubscriber.0;
                subscriptions = [(subscription.namespace, {
                  subscriptionId = subscription.id;
                  config = getSubscriptionSubscriberConfigResponse(subscription.id,thisSubscriber.1);
                  namespace = subscription.namespace;
                  stats = [];
                }
                  )];
                stats = 
                    switch(statistics){
                      case(null) [];
                      case(?stats) getSubscriberStats(thisSubscriber.0, stats);
                    };
              });
              };
            }
          };
        };

        case(?#ByBroadcasterSubscription(principalfilter)){
          label proc for(thisFilter in principalfilter.vals()){
            let ?subscriptionPointer = BTree.get(state.subscriptionsByNamespace, Text.compare, thisFilter.1) else continue proc;
            for(thisSub in Map.entries(subscriptionPointer.0)){
              let ?subscription= BTree.get(state.subscriptions, Nat.compare, thisSub.1) else continue proc;
              //todo: implement proper filter and paging
              for(thisSubscriber in Map.entries(subscription.subscribers)){
                if(Set.has(thisSubscriber.1.registeredBroadcasters, Set.phash, thisFilter.0) == false){
                  continue proc;
                };
                result.add({
                subscriber = thisSubscriber.0;
                subscriptions = [(subscription.namespace, {
                  subscriptionId = subscription.id;
                  config = getSubscriptionSubscriberConfigResponse(subscription.id,thisSubscriber.1);
                  namespace = subscription.namespace;
                  stats = [];
                }
                  )];
                stats = 
                    switch(statistics){
                      case(null) [];
                      case(?stats) getSubscriberStats(thisSubscriber.0, stats);
                    };
              });
              };
            }
          };
        };
      }; */

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_subscribers result " # debug_show(Buffer.toArray(result)));

      return Buffer.toArray(result);
    };

    public func icrc72_get_subscriptions(caller: Principal, params: {
      prev: ?Text;
      take: ?Nat;
      filter: ?OrchestrationFilter;
    }): [SubscriptionInfoResponse] {
      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_subscriptions " # debug_show(params));
      let result = Buffer.Buffer<SubscriptionInfoResponse>(1);
      let start = params.prev;
      let count = getTake(params.take);

      var bFound =  switch(params.prev){
        case(?val) false;
        case(null) true;
      };

      let (slice, statistics) = switch(params.filter){
        case(null) ([],null);
        case(?val) (val.slice, val.statistics);
      };

      let filter = processSlices(slice);


    
      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_subscriptions no filter " # debug_show(params));
      label proc for (item in BTree.entries(state.subscriptions)) {

        if (bFound == false and ?item.1.namespace != start) {
          continue proc;
        };
        bFound := true;

        switch(filter.namespace){
          case(null){};
          case(?namespace){
            if(item.1.namespace != namespace){
              continue proc;
            };
          };
        };


        switch(filter.subscriber){
          case(null){};
          case(?subscriber){
            switch(Map.get(item.1.subscribers, phash, subscriber)){
              case(null) continue proc;
              case(?val){};
            };
          };
        };

        //todo: may need an index for publishers
        /* switch(filter.publisher){
          case(null){};
          case(?publisher){
            switch(Map.get(item.1.publishers, phash, publisher)){
              case(null) continue proc;
              case(?val){};
            };
          };
        };
        */

        switch(filter.broadcaster){
          case(null){};
          case(?broadcaster){
            switch(BTree.get(state.broadcasters, Principal.compare, broadcaster)){
              case(null) continue proc;
              case(?val){
                switch(Map.get(val.subscribers, thash, item.1.namespace)){
                  case(null) continue proc;
                  case(?val){
                    if(Set.size(val.0) == 0){
                      continue proc;
                    };
                  };
                };
              };
            };
          };
        };


        result.add({
          subscriptionId = item.0;
          namespace = item.1.namespace;
          config = getSubscriptionConfigResponse(item.1);
          stats = switch(params.filter){
            case(null) [];
            case(?val){
              switch(val.statistics){
                case(null) [];
                case(?stats) getSubscriptionStats(item.1, stats);
              };
            };
          };
        });
        if (result.size() >=count) {
          break proc;
        }
      };

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_subscriptions result " # debug_show(Buffer.toArray(result)));

      return Buffer.toArray(result);
    };


    public func icrc72_get_broadcasters(caller: Principal, params: {
      prev: ?Principal;
      take: ?Nat;
      filter: ?OrchestrationFilter;
    }): [BroadcasterInfoResponse] {

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_broadcasters " # debug_show(params));
      let result = Buffer.Buffer<BroadcasterInfoResponse>(1);
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
          stats = switch(params.filter){
            case(null) [];
            case(?val) {
              switch(val.statistics){
                case(null) [];
                case(?stats) getBroadcasterStats(item.0, stats);
              };
            };
          };
        });
        if (result.size() >= count) {
          break proc;
        }
      };

      debug if(debug_channel.announce) D.print(".    ORCHESTRATOR: get_broadcasters result " # debug_show(Buffer.toArray(result)));

      return Buffer.toArray(result);
    };

    
    


    ///MARK: Listeners

    type Listener<T> = (Text, T);

    private let publicationRegisteredListeners = Buffer.Buffer<(Text, PublicationRegisteredListener)>(1);

    private let subscriptionRegisteredListeners = Buffer.Buffer<(Text, SubscriptionRegisteredListener)>(1);

    private let subscriberRegisteredListeners = Buffer.Buffer<(Text, SubscriberRegisteredListener)>(1);

    /// Generic function to register a listener.
      ///
      /// Parameters:
      ///     namespace: Text - The namespace identifying the listener.
      ///     remote_func: T - A callback function to be invoked.
      ///     listeners: Vec<Listener<T>> - The list of listeners.
      public func registerListener<T>(namespace: Text, remote_func: T, listeners: Buffer.Buffer<Listener<T>>) {
        let listener: Listener<T> = (namespace, remote_func);
        switch(Buffer.indexOf<Listener<T>>(listener, listeners, func(a: Listener<T>, b: Listener<T>) : Bool {
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