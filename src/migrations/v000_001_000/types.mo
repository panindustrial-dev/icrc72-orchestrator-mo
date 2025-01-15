import Array "mo:base/Array";
import Time "mo:base/Time";
import Principal "mo:base/Principal";
import Star "mo:star/star";
import VectorLib "mo:vector";
import BTreeLib "mo:stableheapbtreemap/BTree";
import SetLib "mo:map/Set";
import MapLib "mo:map/Map";
import TT "../../../../timerTool/src";
import Governance "../../Governance";
import ICRC72PublisherTypes "../../../../icrc72-publisher.mo/src/migrations/types";
import ICRC72Publisher "../../../../icrc72-publisher.mo/src/";
// please do not import any types from your project outside migrations folder here
// it can lead to bugs when you change those types later, because migration types should not be changed
// you should also avoid importing these types anywhere in your project directly from here
// use MigrationTypes.Current property instead


module {

  public let BTree = BTreeLib;
  public let Set = SetLib;
  public let Map = MapLib;
  public let Vector = VectorLib;

  public type Namespace = Text;

  public let GovernanceTypes = Governance;


  public let governance : Governance.Service = actor(Governance.CANISTER_ID);

  public type ICRC16Property = {
        name : Text;
        value : ICRC16;
        immutable : Bool;
    };

    public type ICRC16 = {
        #Array : [ICRC16];
        #Blob : Blob;
        #Bool : Bool;
        #Bytes : [Nat8];
        #Class : [ICRC16Property];
        #Float : Float;
        #Floats : [Float];
        #Int : Int;
        #Int16 : Int16;
        #Int32 : Int32;
        #Int64 : Int64;
        #Int8 : Int8;
        #Map : [(Text, ICRC16)];
        #ValueMap : [(ICRC16, ICRC16)];
        #Nat : Nat;
        #Nat16 : Nat16;
        #Nat32 : Nat32;
        #Nat64 : Nat64;
        #Nat8 : Nat8;
        #Nats : [Nat];
        #Option : ?ICRC16;
        #Principal : Principal;
        #Set : [ICRC16];
        #Text : Text;
    };

    //ICRC3 Value
    public type Value = {
        #Nat : Nat;
        #Int : Int;
        #Text : Text;
        #Blob : Blob;
        #Array : [Value];
        #Map : [(Text, Value)];
    };

    public type ICRC16Map = [(Text, ICRC16)];

    public type PublisherInfo = {
        publisher : Principal; // The principal ID of the publisher canister
        publicationCount : Nat; // Total number of publications made by this publisher
        cyclesReceived : ?Nat; // Optional field to track cycles received from subscribers or for use in micropayments
        messagesSent : Nat; // Total number of messages sent by this publisher
        notifications : Nat; // Total notifications triggered by the publisher's messages
        notificationsConfirmed : Nat; // Total notifications confirmed by subscribers
        subscriberCount : Nat; // Count of subscribers registered for this publisher's messages
    };

    public func equalPublisherInfo(a : PublisherInfo, b : PublisherInfo) : Bool {
        a.publisher == b.publisher and a.publicationCount == b.publicationCount and a.cyclesReceived == b.cyclesReceived and a.messagesSent == b.messagesSent and a.notifications == b.notifications and a.notificationsConfirmed == b.notificationsConfirmed and a.subscriberCount == b.subscriberCount
    };
    public type PublicationRegistration = {
        namespace : Text; // The namespace of the publication for categorization and filtering
        config : ICRC16Map; // Additional configuration or metadata about the publication
        memo: ?Blob;
        // publishers : ?[Principal]; // Optional list of publishers authorized to publish under this namespace
        // subscribers : ?[Principal]; // Optional list of subscribers authorized to subscribe to this namespace
        // mode : Nat; // Publication mode (e.g., sequential, ranked, etc.)
    };

  public type PublicationUpdateRequest = {
     publication : {
      #namespace: Text;
      #publicationId: Nat;
    };
    config : (Text, ICRC16);
    memo: ?Blob;
  };

  public type PublicationDeleteRequest = {
    publication : {
      #namespace: Text;
      #publicationId: Nat;
    };
    memo: ?Blob;
  };

    public type PublicationInfo = {
        namespace : Text; // The namespace of the publication
        initialConfig: ICRC16Map; // Additional configuration or metadata about the publication
        allowedPublishers : Set.Set<Principal>; // List of publishers allowed to publish under this namespace
        disallowedPublishers : Set.Set<Principal>; // List of publishers disallowed from publishing under this namespace
        allowedIcrc75Publishers: ?(Principal, Namespace);
        disallowedIcrc75Publishers: ?(Principal, Namespace);
        allowedSubscribers : Set.Set<Principal>; // List of subscribers allowed to subscribe to this namespace
        disallowedSubscribers : Set.Set<Principal>; // List of subscribers disallowed from subscribing to this namespace
        allowedIcrc75Subscribers: ?(Principal, Namespace);
        disallowedIcrc75Subscribers: ?(Principal, Namespace);

        registeredPublishers : Map.Map<Principal, Set.Set<Principal>>; // Map of publishers registered and their assigned broadcasters
        //todo: These may be handled under a different ICRC eventually...we may have to handle staitiscs a certain way
        var eventCount : Nat;
        var eventsSent : Nat;
        var notifications : Nat;
        var confrimations : Nat;

        stats : BTree.BTree<Text,Value>; // Additional statistics or metadata about the publication
    };

    public type ICRC75Item = {
      principal: Principal;
      namespace: Namespace
    };

    public type PermissionSet = {
        #allowed : Set.Set<Principal>;
        #disallowed : Set.Set<Principal>;
        #allowed_icrc75 : {
          principal: Principal;
          namespace: Namespace
        };
        #disallowed_icrc75 : {
          principal: Principal;
          namespace: Namespace
        };
    };

    public type PermissionSetShared = {
        #allowed : [Principal];
        #disallowed : [Principal];
        #allowed_icrc75 : {
          principal: Principal;
          namespace: Namespace
        };
        #disallowed_icrc75 : {
          principal: Principal;
          namespace: Namespace
        };
    };

    public func permissionSetToshared(p: ?PermissionSet) : ?PermissionSetShared {
      switch (p) {
        case (?#allowed(a)) {
          ?#allowed(Set.toArray(a));
        };
        case (?#disallowed(a)) {
          ?#disallowed(Set.toArray(a));
        };
        case (?#allowed_icrc75(a)) {
          ?#allowed_icrc75(a);
        };
        case (?#disallowed_icrc75(a)) {
          ?#disallowed_icrc75(a);
        };
        case(null) null;
      };
    };

    public type PublisherRecord = {
      broadcasters : Set.Set<Principal>; //canisterID
      var subnet: ?Principal;
    };

    public type PublisherRecordShared = {
      broadcasters : [Principal]; //canisterID
      subnet: ?Principal;
    };

    public func publisherRecordToShared(x : PublisherRecord) : PublisherRecordShared {
      {
        broadcasters = Set.toArray(x.broadcasters);
        subnet = x.subnet;
      }
    };



    public type BroadcasterRecord = {
      publishers : Map.Map<Principal, Set.Set<Text>>; //canisterID, Namespaces
      subscribers : Map.Map<Text, (Set.Set<Principal>, BTree.BTree<Nat, Principal>)>; //Namespace, (stake, principal)
      relays : Map.Map<Text, (Map.Map<Principal, Set.Set<Principal>>, BTree.BTree<Nat, (Principal,Principal)>)>; //Namespace, Map<The Relay Principal, Targets>, BTree<Stake, (target, princpal)>
      subnet: Principal;
    };

    public type BroadcasterRecordShared = {
      publishers : [(Principal, [Text])]; //canisterID, Namespaces
      subscribers : [(Text, ([Principal], [(Nat, Principal)]))]; //Namespace, (stake, principal)
      relays : [(Text, ([(Principal, [Principal])], [(Nat, (Principal,Principal))]))]; //Namespace, Map<The Relay Principal, Targets>, BTree<Stake, (target, princpal)>
      subnet: Principal;
    };

    public func broadcasterRecordToShared(x : BroadcasterRecord) : BroadcasterRecordShared {
      {
        publishers = Array.map<(Principal, Set.Set<Text>),(Principal, [Text])>(Map.toArray(x.publishers), func(entry){
          (entry.0, Set.toArray(entry.1));
        });
        subscribers = Array.map<(Text, (Set.Set<Principal>, BTree.BTree<Nat, Principal>)),(Text, ([Principal], [(Nat, Principal)]))>(Map.toArray(x.subscribers), func(entry){
          (entry.0, (Set.toArray(entry.1.0), BTree.toArray(entry.1.1)));
        });
        relays = Array.map<(Text, (Map.Map<Principal, Set.Set<Principal>>, BTree.BTree<Nat, (Principal,Principal)>)),(Text, ([(Principal, [Principal])], [(Nat, (Principal,Principal))]))>(Map.toArray(x.relays), func(entry){
          (entry.0, (Array.map<(Principal, Set.Set<Principal>),(Principal, [Principal])>(Map.toArray(entry.1.0), func(entry){
            (entry.0, Set.toArray(entry.1));
          }), BTree.toArray(entry.1.1)));
        });
        subnet = x.subnet;
      }
    };

    public type SubscriptionIndex = (Map.Map<Principal, Nat>, BTree.BTree<Nat,Nat>);

    public type SubscriptionRecord = {
      id: Nat;
      publicationId: Nat;
      namespace: Text;
      subscribers: Map.Map<Principal, SubscriberRecord>;
      var stake: Nat;
      controllers: Set.Set<Principal>;
      initialConfig: ICRC16Map;
    };

    public type SubscriptionRecordShared = {
      id: Nat;
      publicationId: Nat;
      namespace: Text;
      subscribers: [(Principal, SubscriberRecordShared)];
      stake: Nat;
      controllers: [Principal];
      initialConfig: ICRC16Map;
    };

    public func subscriptionRecordToShared(x : SubscriptionRecord) : SubscriptionRecordShared {
      {
        id = x.id;
        publicationId = x.publicationId;
        namespace = x.namespace;
        subscribers = Array.map<(Principal, SubscriberRecord),(Principal, SubscriberRecordShared)>(Map.toArray(x.subscribers), func(entry){
          (entry.0, subscriberRecordToShared(entry.1));
        });
        stake = x.stake;
        controllers = Set.toArray(x.controllers);
        initialConfig = x.initialConfig;
      }
    };

    public type SubscriberRecord = {
      subscriptionId: Nat;
      var stake: Nat;
      var filter: ?Text;
      var bStopped: Bool;
      var skip: ?(Nat, Nat);
      subscriber: Principal;
      var subnet: ?Principal;
      registeredBroadcasters : Set.Set<Principal>; 
    };

    public type SubscriberRecordShared = {
      subscriptionId: Nat;
      stake: Nat;
      filter: ?Text;
      bStopped: Bool;
      skip: ?(Nat, Nat);
      subscriber: Principal;
      subnet: ?Principal;
      registeredBroadcasters : [Principal]; 
    };

    public func subscriberRecordToShared(x : SubscriberRecord) : SubscriberRecordShared {
      {
        subscriptionId = x.subscriptionId;
        stake = x.stake;
        filter = x.filter;
        bStopped = x.bStopped;
        skip = x.skip;
        subscriber = x.subscriber;
        subnet = x.subnet;
        registeredBroadcasters = Set.toArray(x.registeredBroadcasters);
      }
    };



    public type PublicationRecord = {
        id : Nat; // Unique identifier for the publication
        namespace : Text; // The namespace of the publication
        initialConfig: ICRC16Map; // Additional configuration or metadata about the publication
        var allowedPublishers : ?PermissionSet; // List of publishers allowed to publish under this namespace
        var allowedSubscribers : ?PermissionSet; // List of subscribers allowed to subscribe to this namespace
        registeredPublishers : Map.Map<Principal, PublisherRecord>; // Map of publishers registered and their assigned broadcasters
        subnetIndex: Map.Map<Principal, Principal>; //subnet, broadcaster
        controllers: Set.Set<Principal>;
    };

    public type PublicationRecordShared = {
        id : Nat; // Unique identifier for the publication
        namespace : Text; // The namespace of the publication
        initialConfig: ICRC16Map; // Additional configuration or metadata about the publication
        allowedPublishers : ?PermissionSetShared; // List of publishers allowed to publish under this namespace
        allowedSubscribers : ?PermissionSetShared; // List of subscribers allowed to subscribe to this namespace
        registeredPublishers : [(Principal, PublisherRecordShared)]; // Map of publishers registered and their assigned broadcasters
        subnetIndex: [(Principal, Principal)]; //subnet, broadcaster
        controllers: [Principal];
    };

    public func publicationRecordToShared(x : PublicationRecord) : PublicationRecordShared{
      {
        id = x.id;
        namespace = x.namespace;
        initialConfig = x.initialConfig;
        allowedPublishers = permissionSetToshared(x.allowedPublishers);
        allowedSubscribers = permissionSetToshared(x.allowedPublishers);
        registeredPublishers = Array.map<(Principal, PublisherRecord),(Principal,PublisherRecordShared)>(Map.toArray(x.registeredPublishers), func(entry){
          (entry.0, publisherRecordToShared(entry.1));
        });
        subnetIndex = Map.toArray(x.subnetIndex);
        controllers = Set.toArray(x.controllers);
      }
    };

    public type PublishError = {
        #Unauthorized;
        #ImproperId : Text;
        #Busy; // This Broadcaster is busy at the moment and cannot process requests
        #GenericError : GenericError;
        #GenericBatchError : Text;
    };

    public type GenericError = {
        error_code : Nat;
        message : Text;
    };


    public type Subscriber = {
        subscriber : Principal; // The principal ID of the subscriber canister
        filter : [Text]; // A list of event filters specifying what types of messages this subscriber wants to receive
    };


    public type SubscriptionInfo = {
        namespace : Text; // The namespace of the subscription
        subscriber : Principal; // Principal ID of the subscriber
        bStopped : Bool; // Indicates whether the subscription is currently active
        filters : [Text]; // Currently active filters for this subscription
        messagesReceived : Nat; // Total number of messages received under this subscription
        messagesRequested : Nat; // Number of messages explicitly requested or queried by the subscriber
        messagesConfirmed : Nat; // Number of messages confirmed by the subscriber (acknowledgment of processing or receipt)
    };

    public func mapValueToICRC16(data : Value) : ICRC16 {
        switch (data) {
            case (#Nat(v)) #Nat(v);
            case (#Nat8(v)) #Nat8(v);
            case (#Int(v)) #Int(v);
            case (#Text(v)) #Text(v);
            case (#Blob(v)) #Blob(v);
            case (#Bool(v)) #Bool(v);
            case (#Array(v)) {
                let result = Vector.new<ICRC16>();
                for (item in v.vals()) {
                    Vector.add(result,mapValueToICRC16(item));
                };
                #Array(Vector.toArray(result));
            };
            case (#Map(v)) {
                let result = Vector.new<(Text, ICRC16)>();
                for (item in v.vals()) {
                    Vector.add(result, (item.0, mapValueToICRC16(item.1)));
                };
                #Map(Vector.toArray(result));
            };
        };
    };

    public type Response = {
        #Ok : Value;
        #Err : Text;
    };

    public type EventRelay = {
        id : Nat;
        prevId : ?Nat;
        timestamp : Nat;
        namespace : Text;
        source : Principal;
        data : ICRC16;
        headers : ?ICRC16Map;
    };

    public type EventNotification = {
        id : Nat;
        eventId : Nat;
        prevEventId : ?Nat;
        timestamp : Nat;
        namespace : Text;
        data : ICRC16;
        source : Principal;
        headers : ?ICRC16Map;
        filter : ?Text;
    };


    public type SubscriberActor = actor {
        icrc72_handle_notification([EventNotification]) : async ();
        icrc72_handle_notification_trusted([EventNotification]) : async {
            #Ok : Value;
            #Err : Text;
        };

    };

    // New Subscriber Types
    type Skip = {
        mod : Nat;
        offset : ?Nat;
    };

    type SubscriptionConfig = ICRC16Map;

    // type SubscriptionRegistration = {
    //     namespace : Text;
    //     config : SubscriptionConfig;
    //     memo : ?Blob;
    // };

    // type SubscriptionInfo = {
    //     subscriptionId : Nat;
    //     subscriber : Principal;
    //     namespace : Text;
    //     config : SubscriptionConfig;
    //     stats : [ICRC16Map];
    // };

    type SubscriptionUpdate = {
        subscriptionId : Nat;
        newConfig : ?SubscriptionConfig;
    };

    type Subscription = {
        subscriptionId : Nat;
        subscriber : Principal;
        namespace : Text;
        config : SubscriptionConfig;
        memo : ?Blob;
        stats : ICRC16Map;
    };

    public type Permission = {
        #Admin;
        #Read;
        #Write;
    };

    public type SubscriptionRegistration = {
      namespace : Text; // The namespace of the publication for categorization and filtering
      config : ICRC16Map; // Additional configuration or metadata about the publication
      memo: ?Blob;
    };

    public type SubscriptionUpdateRequest = {
      subscription : {
        #namespace: Text;
        #id: Nat;
      };
      config : (Text, ICRC16);
      subscriber: ?Principal;
      memo: ?Blob;
    };

    public type SubscriptionUpdateError = {
      #Unauthorized; //generally unauthorized
      #ImproperConfig : Text; //maybe implementation specific
      #GenericError : GenericError;
      #NotFound;
      #GenericBatchError : Text;
    };

    public type SubscriptionUpdateResult = ?{
      #Ok: Bool;
      #Err: SubscriptionUpdateError;
    };


  ///MARK: Serv ERRs

  public type PublicationRegisterError = {
    #Unauthorized; //generally unauthorized
    #UnauthorizedPublisher : {
      namespace : Namespace; //The publisher is not allowed, Look up config by message: Text;
    };
    #NotFound; //The publication does not exist
    #ImproperConfig : Text; //maybe implementation specific
    #GenericError : GenericError;
    //validated
    #Exists : Nat; //The publication already exists and this is its number
    #GenericBatchError : Text;
  };

  public type SubscriptionRegisterError = {
    #Unauthorized; //generally unauthorized
    #ImproperConfig : Text; //maybe implementation specific
    #GenericError : GenericError;
    //validated
    #PublicationNotFound; //The publication does not exist
    #Exists : Nat; //The subscription already exists and this is its number
    #GenericBatchError : Text;
  };

  /////////
  // 1 = candid error
  // 2 = subnet error
  //
  //
  // Publications
  // 1001 - publication not found

  // Subscriptions
  // 2001 - subscription not found
  // 2002 - subscription index error


  // Subscribers
  // 3001 - subscriber required

  ///Mark: Listeners

  /// `Publication Registered Listener`
  ///
  /// Represents a callback function type that notifiers will implement to be alerted to publication registration.
  public type PublicationRegisteredListener = <system>(PublicationRecord, trxid: Nat) -> ();

  public type PublicationDeletedListener = <system>(PublicationRecord, trxid: Nat) -> ();

  /// `Subscription Registered Listener`
  ///
  /// Represents a callback function type that notifiers will implement to be alerted to subscription registration.
  public type SubscriptionRegisteredListener = <system>(SubscriptionRecord, trxid: Nat) -> ();

  /// `Subscriber Registered Listener`
  ///
  /// Represents a callback function type that notifiers will implement to be alerted to subscription registration.
  public type SubscriberRegisteredListener = <system>(SubscriptionRecord, subscriber: Principal, trxid: Nat) -> ();

  ///MARK: Interceptors

  public type CanAddPublication = ?((caller: Principal, publication: PublicationRegistration) -> async* Star.Star<PublicationRegistration, PublicationRegisterError>);

  public type CanAddSubscription = ?((caller: Principal, subscription: SubscriptionRegistration) -> async* Star.Star<SubscriptionRegistration, SubscriptionRegisterError>);

  public type CanUpdatePublication = ?((caller: Principal, publicationSettings: PublicationUpdateRequest) -> async* Star.Star<PublicationUpdateRequest, PublicationRegisterError>);

  public type CanDeletePublication = ?((caller: Principal, publicationSettings: PublicationDeleteRequest) -> async* Star.Star<PublicationDeleteRequest, PublicationRegisterError>);

  public type CanUpdateSubscription = ?((caller: Principal, publicationSettings: SubscriptionUpdateRequest) -> async* Star.Star<SubscriptionUpdateRequest, SubscriptionRegisterError>);

  ///MARK: Constants
  public let CONST = {
    broadcasters = {
      sys = "icrc72:broadcaster:sys:";
      publisher = {
        broadcasters = {
          add = "icrc72:broadcaster:publisher:broadcaster:add";
          remove = "icrc72:broadcaster:publisher:broadcaster:remove";
        };
        add = "icrc72:broadcaster:publisher:add";
        remove = "icrc72:broadcaster:publisher:remove";
      };
      subscriber = {
        add = "icrc72:broadcaster:subscriber:add";
        remove = "icrc72:broadcaster:subscriber:remove";
      };
      relay = {
        add = "icrc72:broadcaster:relay:add";
        remove = "icrc72:broadcaster:relay:remove";
      };
      relayer = {
        add = "icrc72:broadcaster:relayer:add";
        remove = "icrc72:broadcaster:relayer:remove";
      };
    };
    subscription = {
      filter = "icrc72:subscription:filter";
      filter_update = "icrc72:subscription:filter:update";
      filter_remove = "icrc72:subscription:filter:remove";
      bStopped = "icrc72:subscription:bStopped";
      skip = "icrc72:subscription:skip";
      skip_update = "icrc72:subscription:skip:update";
      skip_remove = "icrc72:subscription:skip:remove";
      controllers = {
        list = "icrc72:subscription:controllers";
        list_add = "icrc72:subscription:controllers:list:add";
        list_remove = "icrc72:subscription:controllers:list:remove";
      };
    };

    publication = {
      actions = {
        canAssignBroadcaster = "icrc72:canAssignBroadcaster";
        assignBroadcasterToSubscriber = "icrc72:assignBroadcasterToSubscriber";
      };
      controllers = {
        list = "icrc72:publication:controllers";
        list_add = "icrc72:publication:controllers:list:add";
        list_remove = "icrc72:publication:controllers:list:remove";
      };
      publishers = {
        allowed = {
          list_add = "icrc72:publication:publishers:allowed:list:add";
          list_remove = "icrc72:publication:publishers:allowed:list:remove";
          list = "icrc72:publication:publishers:allowed:list";
          icrc75 = "icrc72:publication:publishers:allowed:icrc75";
          icrc75_remove = "icrc72:publication:publishers:allowed:icrc75:remove";
          icrc75_update = "icrc72:publication:publishers:allowed:icrc75:update";
        };
        disallowed = {
          list_add = "icrc72:publication:publishers:disallowed:list:add";
          list_remove = "icrc72:publication:publishers:disallowed:list:remove";
          list = "icrc72:publication:publishers:disallowed:list";
          icrc75 = "icrc72:publication:publishers:disallowed:icrc75";
          icrc75_remove = "icrc72:publication:publishers:disallowed:icrc75:remove";
          icrc75_update = "icrc72:publication:publishers:disallowed:icrc75:update";
        };
      };
      subscribers = {
        
        allowed = {
          list_add = "icrc72:publication:subscribers:allowed:list:add";
          list_remove = "icrc72:publication:subscribers:allowed:list:remove";
          list = "icrc72:publication:subscribers:allowed:list";
          icrc75 = "icrc72:publication:subscribers:allowed:icrc75";
          icrc75_remove = "icrc72:publication:subscribers:allowed:icrc75:remove";
          icrc75_update = "icrc72:publication:subscribers:allowed:icrc75:update";
        };
        disallowed = {
          list_add = "icrc72:publication:subscribers:disallowed:list:add";
          list_remove = "icrc72:publication:subscribers:disallowed:list:remove";
          list = "icrc72:publication:subscribers:disallowed:list";
          icrc75 = "icrc72:publication:subscribers:disallowed:icrc75";
          icrc75_remove = "icrc72:publication:subscribers:disallowed:icrc75:remove";
          icrc75_update = "icrc72:publication:subscribers:disallowed:icrc75:update";
        };
      };
      broadcasters = {
        sys = "icrc72:broadcaster:sys:";
        publisher = {
          add = "icrc72:broadcaster:publisher:add";
          remove = "icrc72:broadcaster:publisher:remove";
        };
        subscriber = {
          add = "icrc72:broadcaster:subscriber:add";
          remove = "icrc72:broadcaster:subscriber:remove";
        };
        relay = {
          add = "icrc72:broadcaster:relay:add";
          remove = "icrc72:broadcaster:relay:remove";
        };
        relayer = {
          add = "icrc72:broadcaster:relayer:add";
          remove = "icrc72:broadcaster:relayer:remove";
        };
      };
      created = "icrc72:publication:created";
    };
    publishers = {
      sys = "icrc72:publisher:sys:";
    };
    
    subscribers = {
      sys = "icrc72:subscriber:sys:";
    }
  };

  public type InitArgs = {
    name : Text;
  };

  public type Stats = {
    tt : TT.Stats;
    icrc72Publisher : ICRC72Publisher.Stats;
    publications: [(Nat, PublicationRecordShared)];
    subscriptions: [(Nat, SubscriptionRecordShared)];
    broadcasters: [(Principal, BroadcasterRecordShared)];
    nextPublicationID: Nat;
    nextSubscriptionID: Nat;
    defaultTake: Nat;
    maxTake: Nat;
  };

  public type Environment = {
    addRecord: ?(([(Text, Value)], ?[(Text,Value)]) -> Nat);
    tt: TT.TimerTool;
    icrc72Publisher : ICRC72Publisher.Publisher;
  };

  ///MARK: State
  public type State = {
    publications : BTree.BTree<Nat, PublicationRecord>;
    publicationsByNamespace : BTree.BTree<Text, Nat>;
    broadcasters : BTree.BTree<Principal, BroadcasterRecord>; //subnet, record
    broadcastersBySubnet : Map.Map<Principal, Vector.Vector<Principal>>; //subnet, broadcaster 
    subscribersByPrincipal : BTree.BTree<Principal, Set.Set<Nat>>; //principal, subscriptionID
    subscriptions: BTree.BTree<Nat, SubscriptionRecord>;
    subscriptionsByNamespace : BTree.BTree<Text, SubscriptionIndex>; //namespace, ((subscriber,subscription), (stake, subscription))
    var nextPublicationID : Nat;
    var nextSubscriptionID : Nat;
    var defaultTake : Nat;
    var maxTake : Nat;
  };
};