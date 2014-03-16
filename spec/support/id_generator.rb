module IdGenerator
  def generate_id
    (Time.now.to_i * 10**10) + rand(10**10)
  end
end
