/*
 * BSD 2-Clause License
 *
 * Copyright (c) 2017, Redis Labs
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.redislabs.modules.rejson;

import com.google.gson.Gson;

import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

/**
 * JReJSON is the main ReJSON client class, wrapping connection management and all ReJSON commands
 */
public class JReJSON {

    private static Gson gson = new Gson();

    private enum Command implements ProtocolCommand {
        DEL("JSON.DEL"),
        GET("JSON.GET"),
        SET("JSON.SET"),
        TYPE("JSON.TYPE");
        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    /**
     * Existential modifier for the set command, by default we don't care
     */
    public enum ExistenceModifier implements ProtocolCommand {
        DEFAULT(""),
        NOT_EXISTS("NX"),
        MUST_EXIST("XX");
        private final byte[] raw;

        ExistenceModifier(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    /**
     *  Helper to check for errors and throw them as an exception
     * @param str the reply string to "analyze"
     * @throws RuntimeException
     */
    private static void assertReplyNotError(final String str) {
        if (str.startsWith("-ERR"))
            throw new RuntimeException(str.substring(5));
    }

    /**
     * Helper to check for an OK reply
     * @param str the reply string to "scrutinize"
     */
    private static void assertReplyOK(final String str) {
        if (!str.equals("OK"))
            throw new RuntimeException(str);
    }
    
    /**
     * Helper to check for an QUEUED reply
     * @param str the reply string to "scrutinize"
     */
    private static void assertReplyQUEUED(final String str) {
        if (!str.equals("QUEUED"))
            throw new RuntimeException(str);
    }
    

    /**
     * Helper to handle single optional path argument situations
     * @param path a single optional path
     * @return the provided path or root if not
     */
    private static Path getSingleOptionalPath(Path... path) {
        // check for 0, 1 or more paths
        if (1 > path.length)
            // default to root
            return Path.RootPath();
         else if (1 == path.length)
            // take 1
            return path[0];
         else
            // throw out the baby with the water
            throw new RuntimeException("Only a single optional path is allowed");
    }

    /**
     * Deletes a path
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the number of paths deleted (0 or 1)
     */
    public static Long del(Jedis conn, String key, Path... path) {

        List<byte[]> args = new ArrayList(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        //TODO:DJC
        Client client = conn.getClient(); 
        client.sendCommand(Command.DEL, args.toArray(new byte[args.size()][]));
        Long rep =  client.getIntegerReply();
        
        conn.close();

        return rep;
    }

    /**
     * Gets an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param paths optional one ore more paths in the object, defaults to root
     * @return the requested object
     */
    public static Object get(Jedis conn, String key, Path... paths) {

        List<byte[]> args = new ArrayList(2);

        args.add(SafeEncoder.encode(key));
        for (Path p :paths) {
            args.add(SafeEncoder.encode(p.toString()));
        }

        
        //TODO: DJC
        Client client = conn.getClient();
        
        client.sendCommand(Command.GET, args.toArray(new byte[args.size()][]));
        String rep = client.getBulkReply();
        
        //conn.close();

        assertReplyNotError(rep);
        return gson.fromJson(rep, Object.class);
    }

    
    /**
     * Sets an object (with expiry)
     * @param conn the Jedis connection
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     * @param expirySeconds the key will timeout after a given number of seconds
     * @param path optional single path in the object, defaults to root

     */
    public static void set(Jedis conn, String key, Object object, ExistenceModifier flag, int expirySeconds,  Path... path) {
        List<byte[]> args = new ArrayList(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        if (ExistenceModifier.DEFAULT != flag) {
            args.add(flag.getRaw());
        }

        //TODO: DJC
//        if (expirySeconds > 0) {
//          args.add(SafeEncoder.encode("EX"));
//          args.add(SafeEncoder.encode(String.valueOf(expirySeconds)));
//        }
        Client client = conn.getClient();

        
        //client.sendCommand(Protocol.Command.MULTI);
        String stat; 
        client.multi();
        stat = client.getStatusCodeReply();
        assertReplyOK(stat);
        
        client.sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
        stat = client.getStatusCodeReply();
        assertReplyQUEUED(stat);
        
        
        //client.sendCommand(Protocol.Command.EXPIRE, SafeEncoder.encode(key), SafeEncoder.encode(String.valueOf(expirySeconds)));
        client.expire(key, expirySeconds);
        stat = client.getStatusCodeReply();
        assertReplyQUEUED(stat);
        
       
        //client.sendCommand(Protocol.Command.EXEC);
        client.exec();
        
        //stat = client.getStatusCodeReply();
        //assertReplyQUEUED(stat);
        
        List<Object> responseList = client.getObjectMultiBulkReply();
        //Check results:
        if (responseList.size() != 2) {
          throw new RuntimeException("An error occurred while setting with expiry.");
        }
        
        Object o1 = responseList.get(0);
        
        if (!o1.getClass().isArray() ||
            !(o1  instanceof byte[] ) ||
            !SafeEncoder.encode((byte[])o1).equals("OK"))
        {
          throw new RuntimeException("SET COMMAND FAILED.");
        }
        
        //assertReplyOK((String)));

        // check the expire was successful:
        if ((Long)responseList.get(1) != 1) {
          throw new RuntimeException("EXPIRE COMMAND FAILED.");
        }
          


        
        
        
        
        
        
        
        
        ///client.sendCommand(Protocol.Command.EXPIRE, String.valueOf(expirySeconds));
        //status =  client.getStatusCodeReply();
        
        
        //removed this close so I can use the pool:
        //  see: https://github.com/RedisLabs/JReJSON/issues/4
        //conn.close();

        
        //TODO: figure out what the respose is for exec:
        
        //String status =  client.getStatusCodeReply();
        //assertReplyOK(status);
    }
    
    
    /**
     * Sets an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     * @param path optional single path in the object, defaults to root
     */
    public static void set(Jedis conn, String key, Object object, ExistenceModifier flag, Path... path) {

        List<byte[]> args = new ArrayList(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        if (ExistenceModifier.DEFAULT != flag) {
            args.add(flag.getRaw());
        }

        //TODO: DJC
        Client client = conn.getClient();
        client.sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
        String status =  client.getStatusCodeReply();
        
        conn.close();

        assertReplyOK(status);
    }

    /**
     * Sets an object without caring about target path existing
     * @param conn the Jedis connection
     * @param key the key name
     * @param object the Java object to store
     * @param path optional single path in the object, defaults to root
     */
    public static void set(Jedis conn, String key, Object object, Path... path) {
        set(conn,key, object, ExistenceModifier.DEFAULT, path);
    }

    /**
     * Gets the class of an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the Java class of the requested object
     */
    public static Class<?> type(Jedis conn, String key, Path... path) {

        List<byte[]> args = new ArrayList(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        //TODO:DJC
        Client client = conn.getClient(); 
        client.sendCommand(Command.TYPE, args.toArray(new byte[args.size()][]));
        String rep = client.getBulkReply();
        
        conn.close();

        assertReplyNotError(rep);

        switch (rep) {
            case "null":
                return null;
            case "boolean":
                return boolean.class;
            case "integer":
                return int.class;
            case "number":
                return float.class;
            case "string":
                return String.class;
            case "object":
                return Object.class;
            case "array":
                return List.class;
            default:
                throw new java.lang.RuntimeException(rep);
        }
    }
}
