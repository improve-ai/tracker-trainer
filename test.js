const Choosy = require("choosy-ai")
const choosy = new Choosy("https://foo.com/bar/latest/models.tar.gz")

let greeting;
greeting = choosy.choose(["hi", "howdy"])
greeting = choosy.choose(["hi", "howdy"], null, "default") // same as previous
greeting = choosy.choose(["hi", "howdy"], {language: "cowboy"})
greeting = choosy.choose(["hi", "howdy"], {language: "cowboy"}, "default") // same as previous
greeting = choosy.choose(["hi", "howdy"], {language: "cowboy"}, "greetings")
greeting = choosy.choose(["hi", "howdy"], {language: "cowboy"}, "greetings", "revenue")
greeting = choosy.choose(["hi", "howdy"], {language: "cowboy"}, "greetings", null, false)
greeting = choosy.choose({variants: ["hi", "howdy"], context: {language: "cowboy"}, domain: "greetings", rewardKey: "revenue", autoTrack: true})

choosy.trackChosen(greeting, {language: "cowboy"}, "greetings", "revenue")
choosy.trackChosen({variant: greeting, context: {language: "cowboy"}, domain: "greetings", rewardKey: "revenue"})

let sorted;
sorted = choosy.sort(["hi", "howdy"])
sorted = choosy.sort(["hi", "howdy"], {language: "cowboy"})
sorted = choosy.sort(["hi", "howdy"], {language: "cowboy"}, "default")
sorted = choosy.sort({variants: ["hi", "howdy"], context: {language: "cowboy"}, domain: "greetings"})

choosy.trackReward(1.0) // equivilent to trackRewards({"default": 1.0})
choosy.trackRewards({"greetings": 1.0})
choosy.trackRewards({"greetings": 1.0, "hi": 1.0})
choosy.trackRewards({"greetings": 1.0, "hi": 1.0}, {mode: "set"})
choosy.trackRewards({"$set": {"greetings": 1.0}, "hi": 1.0})

choosy.trackAnalyticsEvent("Greeting Shown", {greeting: "hi"})


let arranged;
arranged = choosy.arrange(["hi", "howdy"], {language: "cowboy"}, "default", 2, 4)
arranged = choosy.arrange({variants: ["hi", "howdy"], context: {language: "cowboy"}, domain: "default", minValues: 2, maxValues:4})

let configuration;
configuration = choosy.apply([{"choose": ["the", "key", "path"]}], {"the": {"key": {"path": ["hi", "howdy"]}}})

ChoosyAI *choosy = [ChoosyAI instance:@"greetings"];
NSString *greeting = [choosy choose:@[@"hi", @"howdy"] context:@{@"language": @"cowboy"}]; // why not autoTrackDecision???  I can't remember, maybe it was because of nil namespaces?  sort is an issue
[choosy trackDecision:greeting context:@{@"language": @"cowboy"}];
[choose addReward:1.0];