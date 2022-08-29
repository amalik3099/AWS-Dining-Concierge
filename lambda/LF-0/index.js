var aws = require('aws-sdk');
aws.config.update({region: 'us-east-1'});
var lex = new aws.LexRuntime({apiVersion: '2016-11-28'});
function uuidv4() {
    // Source: https://stackoverflow.com/questions/105034/how-to-create-a-guid-uuid
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
}
function postText(userId, text, sessionAttributes) {
    return new Promise((resolve, reject) => {
        const params = {
            botAlias: 'dine',
            botName: 'DiningConcierge',
            inputText: text,
            userId: userId,
            sessionAttributes: sessionAttributes
        };
        
        lex.postText(params, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
        
    });
}
exports.handler = async (event) => {
    
    console.log(event);
    
    const userId = event.userId;"testUser"; //uuidv4();
    const messages = event.messages;
    const sessionAttributes = event.sessionAttributes;
    const textType = messages[0].type;
    const text = messages[0][textType].text;
    const res = await postText(userId, text, sessionAttributes);
    console.log(res)
    
    const response = {
        statusCode: 200,
        sessionAttributes: res.sessionAttributes,
        messages: [{
            type: 'unstructured',
            unstructured: {
                text: res.message
            }
        }]
    };
    return response;
};