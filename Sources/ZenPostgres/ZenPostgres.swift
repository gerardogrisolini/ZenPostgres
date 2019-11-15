//
//  ZenPostgres.swift
//  ZenPostgres
//
//  Created by Gerardo Grisolini on 17/03/2019.
//

import Foundation
import PostgresKit


public protocol Database {
    func connect() -> EventLoopFuture<PostgresConnection>
    func disconnect(_ connection: PostgresConnection)
    func close() throws
}

public class ZenPostgres: Database {
    public static var pool: ZenPostgres!

    private let eventLoopGroup: EventLoopGroup
    private let pool: ConnectionPool<PostgresConnectionSource>
    
    public init(config: PostgresConfig, numberOfThreads: Int = System.coreCount) throws {
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: numberOfThreads)
        let db = PostgresConnectionSource(
            configuration: .init(hostname: config.host, username: config.username, password: config.password, database: config.database)
        )
        pool = ConnectionPool(configuration: .init(maxConnections: config.maximumConnections), source: db, on: self.eventLoopGroup)
        ZenPostgres.pool = self
    }
   
    public init(config: PostgresConfig, eventLoopGroup: EventLoopGroup) throws {
        self.eventLoopGroup = eventLoopGroup
        let db = PostgresConnectionSource(
            configuration: .init(hostname: config.host, username: config.username, password: config.password, database: config.database)
        )
        pool = ConnectionPool(configuration: .init(maxConnections: config.maximumConnections), source: db, on: self.eventLoopGroup)
        ZenPostgres.pool = self
    }

    public func newPromise<T>() -> EventLoopPromise<T> {
        return eventLoopGroup.next().makePromise(of: T.self)
    }

    public func connect() -> EventLoopFuture<PostgresConnection> {
        return pool.requestConnection().map { conn -> PostgresConnection in
            #if DEBUG
            print("CONNECT")
            #endif
            
            return conn
        }
    }
    
    public func disconnect(_ connection: PostgresConnection) {
        #if DEBUG
        print("DISCONNECT")
        #endif

        pool.releaseConnection(connection)
    }

    public func close() throws {
        #if DEBUG
        print("CLOSE")
        #endif

        pool.shutdown()
        try eventLoopGroup.syncShutdownGracefully()
    }
}

extension PostgresConnection {
    public func disconnect() {
        ZenPostgres.pool.disconnect(self)
    }
}

extension String {
    func capitalizingFirstLetter() -> String {
        return prefix(1).capitalized + dropFirst()
    }
    
    mutating func capitalizeFirstLetter() {
        self = self.capitalizingFirstLetter()
    }
}

public enum ZenError: Error {
    case connectionNotFound
    case recordNotFound
    case recordNotSave
    case passwordDoesNotMatch
    case error(_ message: String)
}

public struct DataSourceJoin {
    public let table:            String
    public let direction:        JoinType
    public let onCondition:      String
    
    public init(table: String, onCondition: String = "", direction: JoinType = .INNER) {
        self.table = table
        self.direction = direction
        self.onCondition = onCondition
    }
}

public enum JoinType {
    case INNER
    case OUTER
    case LEFT
    case RIGHT
}

public struct Cursor {
    public var limit:           Int = 50
    public var offset:          Int = 0
    public var totalRecords:    Int = 0
    public init() {}
    
    public init(limit: Int, offset: Int) {
        self.limit      = limit
        self.offset     = offset
    }
    
    public init(limit: Int, offset: Int, totalRecords: Int) {
        self.limit          = limit
        self.offset         = offset
        self.totalRecords   = totalRecords
    }
}


