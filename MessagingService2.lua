-- Improved Messaging Service
-- Username
-- March 8, 2021


local MessagingService = game:GetService("MessagingService")
local http = game:GetService("HttpService")
local MessagingService2 = {}

function MessagingService2:PublishAsync(topic, message)
     --[[
        input: message = Tbl, transferred data, disclaimer: message < 1kb
    ]]
    MessagingService:PublishAsync(topic, http:JSONEncode(message))
end

function MessagingService2:SubscribeAsync(Topic, func)
    --[[
        callbackfunction()'s message.Data = tbl, decoded JSON
    ]]
    local subscribeConnection = MessagingService:SubscribeAsync(Topic, function(message)
        message.Data = http:JSONDecode(message.Data)
        func(message)
    end)
    return subscribeConnection
end

function MessagingService2:safePublish(topic, message)
    --[[
        input: message = Tbl, transferred data, disclaimer: message < 1kb

        Try PublishAsync, if Fails, retry 10 times 
        if failed 10 times then kick player
    ]]

    for i = 1, 10 do
        local publishSuccess, publishResult = pcall(function()
            MessagingService:PublishAsync(topic, message)
        end)
        if publishSuccess then
            return true
        end
        print("Failed to Publish to Topic: ".. topic.. ". RetryCount = ".. tostring(i))
        wait()
    end

    return false
end

function MessagingService2:safeSubscribe(Topic, func)
    --[[
        input: Topic, func = inputs required in MessagingService:SubscribeAsync    

        Try subscribeAsync, if Fails, retry 10 times
        if failed 10 times then kick player
    ]]

    -- Subscribe to Invite topic
    for i = 1, 10 do
        local subscribeSuccess, subscribeConnection = pcall(function()
            return MessagingService:SubscribeAsync(Topic, func)
        end)
        if subscribeSuccess then
            return subscribeConnection
        end
        print("Failed to subscribe to Topic: ".. Topic.. ". RetryCount = ".. tostring(i))
        wait()
    end

    return false
end

function MessagingService2:invokeTopic(topic, message, maximumWaitingTime)
    --[[
        input: message = tbl, maximumWaitingTime = maximum time the invoke will yield, returns nil after **optional**
        invokes another server yielding until it returns a value

        returns: value which another server with onTipicInvoked returned
    ]]
    local startTime = os.time()
    local messageId = http:GenerateGUID(false)
    local returnData = nil
    local returned = false

    local connection;
    connection = self:safeSubscribe(topic, function(incomingMessage)
        if typeof(incomingMessage.Data) == "table" then
            local key = incomingMessage.Data[1]
            local incomingMessageId = incomingMessage.Data[2]
            local returnerId = incomingMessage.Data[3]
            local data = incomingMessage.Data[4]
            if key == "return" and messageId == incomingMessageId and not returned then -- check if message is to return this invoke
                -- publishing received returnerId
                returned = true
                returnData = data
                self:safePublish(messageId, returnerId)
            end
        end
    end)
    self:safePublish(topic, {"invoke", messageId, message})
    
    repeat wait() until returned or (os.time() - startTime) > maximumWaitingTime
    connection:Disconnect()

    -- if not returned received, publish returnerId as nil to disconnect events
    if not returned then
        self:safePublish(messageId, nil)
    end

    return returnData
end

function MessagingService2:onTopicInvoked(topic, invokedFunc, returnReceivedCallback)
    --[[
       
        input:    
            topic: str
            invokedFunc(data, returnerId) -  fires upon topic invoke and returns a value
                input: data = message.Data, returnerId = Id of current callback return message
            returnReceivedCallback(data, returnerId, receivedReturnerId) -- fires when first return is received by the Invoker
                data: message.Data, same data the invokedFunc received, returnerId = returnerId = Id of current callback return message, receivedReturnerId = returnerId first received by Invoker
        
        NOTE: 
            return *any value* -> invoker will stop yielding and return this value
            return nil -> invoker will keep yielding for other servers to return 
            return "nil" -> invoker will stop yielding and return nil
           
        This can be used to run a check function where only a specified server responds

        returns: MessagingService subscribeConnection
    ]]
    local connection = self:safeSubscribe(topic, function(message)
        if typeof(message.Data) == "table" then
            local key = message.Data[1]
            if key == "invoke" then
                local messageId = message.Data[2]
                local data = message.Data[3]
                local returnerId = http:GenerateGUID(false)
                local returnValue;

                -- fires returnReceivedCallback function when this specific invoke has received 1 return
                returnReceivedConnection = self:safeSubscribe(messageId, function(message)
                    --[[
                        input: message.Data = str, UUID, receivedReturnerId
                    ]]
                    returnReceivedConnection:Disconnect()
                    if returnValue then
                        local receivedReturnerId = message.Data
                        print("returnReceivedCallback Message Id: ", messageId)
                        returnReceivedCallback(data, returnerId, receivedReturnerId)
                    end
                end)

                print("invokedFunc Message Id: ", messageId)
                returnValue = invokedFunc(data, returnerId)
                if returnValue then
                    if returnValue == "nil" then
                        self:safePublish(topic, {"return", messageId, returnerId, nil})
                    else
                        self:safePublish(topic, {"return", messageId, returnerId, returnValue})
                    end
                end
            end
        end
    end)
    return connection
end

return MessagingService2