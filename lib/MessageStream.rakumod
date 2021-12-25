use     JSON::Marshal;
use     JSON::Unmarshal;
need    MessageStream::Message;
unit    role MessageStream:api<1>:auth<Mark Devine (mark@markdevine.com)>;

has     Supplier    $.supplier  = Supplier.new;
has     Supply      $.supply;
has     Tap         %.taps;
has     Lock::Async $.lock      = Lock::Async.new;

submethod TWEAK {
    $!supply = $!supplier.Supply;
}

method post ($payload? is copy, *%options) {
    if $payload {
        if $payload.WHAT ~~ Positional {
            if $payload.WHAT !=== Array[Str] {
                my @p;
                for $payload.list -> $p {
                    @p.push: $p.Str;
                }
                $payload = @p;
            }
        }
    }
    else {
        $payload = '';
    }
    $!lock.protect: {
        $!supplier.emit: marshal(MessageStream::Message.new(:$payload, :%options));
    }
}

method subscribe (Str:D :$destination) {
    my $method-name = $destination ~ '-receive';
    die "Unable to subscribe: First implement method $method-name (MessageStream::Message:D \$message) in your code" unless self.can($method-name);
    %!taps{$destination} = $!supply.tap: -> $m { self."$method-name"(unmarshal($m, MessageStream::Message)) };
}

method unsubscribe (Str:D :$destination) {
    return unless %!taps{$destination}:exists;
    %!taps{$destination}.close;
    %!taps{$destination}:delete;
}

=finish
